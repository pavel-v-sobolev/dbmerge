<p align="left">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/pavel-v-sobolev/dbmerge/master/assets/logo_dark.png">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/pavel-v-sobolev/dbmerge/master/assets/logo_light.png">
    <img alt="dbmerge logo" src="https://raw.githubusercontent.com/pavel-v-sobolev/dbmerge/master/assets/logo_light.png" width="500">
  </picture>
</p>

**DBMerge** is a Python library that provides a simplified interface for performing `UPSERT` (Insert/Update/Delete) operations. \
Built on top of SQLAlchemy, it abstracts away the complexities of writing engine-specific SQL `MERGE` or `ON CONFLICT` statements.

[![PyPI version](https://img.shields.io/pypi/v/dbmerge.svg)](https://pypi.org/project/dbmerge/)
[![Python versions](https://img.shields.io/pypi/pyversions/dbmerge.svg)](https://pypi.org/project/dbmerge/)

[![PostgreSQL](https://github.com/pavel-v-sobolev/dbmerge/actions/workflows/test_postgresql.yml/badge.svg)](https://github.com/pavel-v-sobolev/dbmerge/actions/workflows/test_postgresql.yml) [![MariaDB](https://github.com/pavel-v-sobolev/dbmerge/actions/workflows/test_mariadb.yml/badge.svg)](https://github.com/pavel-v-sobolev/dbmerge/actions/workflows/test_mariadb.yml) [![SQLite](https://github.com/pavel-v-sobolev/dbmerge/actions/workflows/test_sqlite.yml/badge.svg)](https://github.com/pavel-v-sobolev/dbmerge/actions/workflows/test_sqlite.yml) [![MS SQL](https://github.com/pavel-v-sobolev/dbmerge/actions/workflows/test_mssql.yml/badge.svg)](https://github.com/pavel-v-sobolev/dbmerge/actions/workflows/test_mssql.yml)



## Overview

**DBMerge accepts multiple data sources as input:**
- Pandas DataFrames
- Lists of dictionaries
- Other database tables or views

DBMerge automates data update process by comparing your source data against the target table and automatically performing the required operations.

- **Insert** new records that do not exist in the target table.
- **Update** existing records only if their values have changed.
- **Delete (or mark)** existing records in the target table that are no longer present in the source data.

To ensure optimal performance, the library loads your data into a temporary table first, and then executes bulk synchronization queries.

## Supported Databases
Tested and verified with:
- PostgreSQL
- MariaDB / MySQL
- SQLite
- MS SQL Server

## Installation

```bash
pip install dbmerge
```

## Quick Start Example

The library uses a context manager to handle database connections and ensure resources are safely released.

```python
from sqlalchemy import create_engine
from datetime import date
from dbmerge import dbmerge

# 1. Initialize DB engine
engine = create_engine("sqlite://")

# 2. Prepare your source data
data = [
    {'Shop': '123', 'Product': 'A1', 'Date': date(2025, 1, 1), 'Qty': 2, 'Price': 50.10},
    {'Shop': '124', 'Product': 'A1', 'Date': date(2025, 1, 1), 'Qty': 1, 'Price': 100.50}
]

# 3. Execute the merge operation
# The table will be created automatically if it doesn't exist.
with dbmerge(engine=engine, data=data, table_name="Facts", 
             key=['Shop', 'Product', 'Date']) as merge:
    merge.exec()
```

## Key Features

- **Database Agnostic:** Write your synchronization logic once and run it across different SQL databases without modifying the code.
- **High Performance:** Uses temporary staging tables for fast bulk operations rather than slow row-by-row changes.
- **Smart Deletion:** Supports scoped deletion. You can pass a SQLAlchemy logical expression to delete missing data only within a specific timeframe or subset (e.g., updating only a single month).
- **Auto-Schema Management:** Automatically creates missing tables or columns in the database.
- **Audit:** Optional parameters to automatically add `merged_on` and `inserted_on` timestamps to track when rows were created or modified.


## Benchmark

DBMerge handles the entire reconciliation process (staging, comparing, updating, inserting) with solid performance, scaling well even for larger datasets.

Here is a rough performance comparison for synchronizing data of different sizes using DBMerge (measured on a standard developer laptop):

| Database | DBMerge (100k rows) | DBMerge (1mil rows) |
|----------|---------------------|---------------------|
| **PostgreSQL** | ~2.0s | ~19.8s |
| **MySQL / MariaDB** | ~1.0s | ~11.1s |
| **SQLite** | ~0.7s | ~7.6s |
| **MS SQL Server*** | ~22.4s | ~4m 23s |

*\* Note: MS SQL Server bulk operations take longer due to inherent limitations in the `pyodbc` driver*


## Database-Specific Notes & Limitations

- **PostgreSQL:** 
  - Temporary tables are created as `UNLOGGED`. 
  - `JSONB` type is supported, but not `JSON` (as it cannot be compared to detect changes).
- **MariaDB / MySQL:** 
  - Does not detect changes in uppercase vs. lowercase or space padding by default (e.g., `'test' == ' Test'`). If this is important, you need to change the collation settings in your database.
  - The schema is treated the same as the database, but schema settings are still supported by this library.
  - Does not allow strings with unlimited size. You must explicitly define `data_types` if you want to create a table or field automatically (e.g., `data_types={'Your Field': String(100)}`).
- **SQLite:** Does not support schemas. If a schema setting is provided, it is automatically reset to `None` with a warning.
- **MS SQL Server:** Bulk insert operations may have lower performance due to specific `pyodbc` driver limitations.
- **Oracle:** Currently not supported (missing support for `JOIN` operations in `UPDATE` statements within the `oracledb` module).
- **DuckDB:** Currently not supported (due to a bug in `duckdb_engine` regarding table definition loading).

## Documentation

- [Full Module Documentation](https://github.com/pavel-v-sobolev/dbmerge/blob/main/DOCUMENTATION.md)
- [Advanced Examples (Python)](https://github.com/pavel-v-sobolev/dbmerge/blob/main/user_guide.py)