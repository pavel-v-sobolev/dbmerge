"""Reproduce audit observations locally, without connecting to external databases.

Run from the repository root: PYTHONPATH=src .venv/bin/python audit/recheck_2_0_0.py
The output records observations, not a pass/fail regression suite. PostgreSQL
checks use installed dialects and CSV conversion only; they do not exercise COPY
against a server. SQLite databases exist only for the lifetime of this process.
"""

import ast
import importlib.metadata
import json
import logging
import platform
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
from sqlalchemy import Column, Integer, JSON, MetaData, Table, create_engine, null, text
from sqlalchemy.pool import NullPool, StaticPool

from dbmerge import _bulk_load, dbmerge


def error_info(exc):
    return {"type": type(exc).__name__, "message": str(exc).splitlines()[0]}


def sqlite_engine(**kwargs):
    return create_engine("sqlite:///:memory:", poolclass=kwargs.pop("poolclass", StaticPool), **kwargs)


def input_shape(data):
    engine = sqlite_engine()
    try:
        with dbmerge(engine=engine, data=data, table_name="shape", key=["id"]) as merge:
            merge.exec()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM shape ORDER BY id"))
            return {"columns": list(result.keys()), "rows": [list(row) for row in result]}
    finally:
        engine.dispose()


def unique_transition():
    engine = sqlite_engine()
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql("CREATE TABLE target (id INTEGER PRIMARY KEY, code TEXT UNIQUE)")
            conn.exec_driver_sql("INSERT INTO target VALUES (1, 'A'), (2, 'B')")
        try:
            with dbmerge(engine=engine, data=[{"id": 1, "code": "B"}], table_name="target",
                         key=["id"], delete_mode="delete") as merge:
                merge.exec(commit_all_steps=False)
        except Exception as exc:
            return error_info(exc)
        return {"unexpected_success": True}
    finally:
        engine.dispose()


def generated_column():
    engine = sqlite_engine()
    try:
        with engine.begin() as conn:
            for name in ("source", "target"):
                conn.exec_driver_sql(f"CREATE TABLE {name} (id INTEGER PRIMARY KEY, a INTEGER, "
                                     "doubled INTEGER GENERATED ALWAYS AS (a * 2) STORED)")
            conn.exec_driver_sql("INSERT INTO source (id, a) VALUES (1, 3)")
        try:
            with dbmerge(engine=engine, source_table_name="source", table_name="target", key=["id"]) as merge:
                merge.exec()
        except Exception as exc:
            return error_info(exc)
        return {"unexpected_success": True}
    finally:
        engine.dispose()


def cleanup_failure():
    engine = sqlite_engine()
    merge = dbmerge(engine=engine, data=[{"id": 1}], table_name="cleanup", key=["id"])
    try:
        with patch.object(merge, "_insert_data_to_temp", side_effect=RuntimeError("PRIMARY_FAILURE")), \
             patch.object(merge, "_drop_temp_table", side_effect=RuntimeError("CLEANUP_FAILURE")):
            try:
                merge.exec()
            except Exception as exc:
                return {**error_info(exc), "context": str(exc.__context__),
                        "connection_closed": merge.conn.closed, "merge_finished": merge.merge_finished}
    finally:
        merge._drop_temp_table()
        merge.conn.close()
        engine.dispose()


def nullpool_create():
    engine = sqlite_engine(poolclass=NullPool)
    try:
        with dbmerge(engine=engine, data=[{"id": 1}], table_name="new_target", key=["id"]) as merge:
            result = merge.exec()
        return {"inserted": result.inserted_row_count}
    finally:
        engine.dispose()


def mark_contract():
    engine = sqlite_engine()
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql("CREATE TABLE target (id INTEGER PRIMARY KEY, value TEXT, deleted INTEGER)")
            conn.exec_driver_sql("INSERT INTO target VALUES (1, 'old', 1)")
        with dbmerge(engine=engine, data=[{"id": 1, "value": "new"}], table_name="target", key=["id"],
                     delete_mode="mark", delete_mark_field="deleted") as merge:
            merge.exec(update_condition=merge.table.c.id == 999)
        with engine.connect() as conn:
            protected = list(conn.exec_driver_sql("SELECT * FROM target").one())
        with dbmerge(engine=engine, data=[{"id": 1, "value": "new", "deleted": None}],
                     table_name="target", key=["id"], delete_mode="mark", delete_mark_field="deleted") as merge:
            merge.exec()
        with engine.connect() as conn:
            supplied_null = list(conn.exec_driver_sql("SELECT * FROM target").one())
        return {"protected_row": protected, "supplied_null": supplied_null}
    finally:
        engine.dispose()


def json_csv():
    results = {}
    for driver in ("psycopg2", "psycopg", "pg8000"):
        engine = create_engine(f"postgresql+{driver}://")
        try:
            for none_as_null in (False, True):
                from sqlalchemy.dialects.postgresql import JSONB
                column_type = JSONB(none_as_null=none_as_null)
                rows = [{"id": i, "doc": value} for i, value in enumerate(
                    ({"a": 1}, [1, None], None, JSON.NULL, null()))]
                merge = SimpleNamespace(
                    conn=SimpleNamespace(dialect=engine.dialect), data=rows,
                    data_fields={"id": Integer(), "doc": column_type}, type_of_data="list of dict",
                    temp_table=Table("staging", MetaData(), Column("id", Integer), Column("doc", column_type)))
                processors = _bulk_load._json_processors(merge)
                results[f"{driver}:none_as_null={none_as_null}"] = {
                    "bind_processor_is_none": column_type.bind_processor(engine.dialect) is None,
                    "processed": [processors["doc"](row["doc"]) for row in rows],
                    "csv": _bulk_load._csv_by_csv_module(merge, 0, len(rows), processors, {}).decode(),
                    "arrow_csv": _bulk_load._csv_by_arrow(merge, 0, len(rows), processors, {}).decode(),
                }
        finally:
            engine.dispose()
    return results


def copy_transaction_tracking():
    # The SQLAlchemy connection is real, but the COPY call is replaced. This
    # isolates transaction registration without pretending to test PostgreSQL.
    engine = sqlite_engine()
    try:
        with engine.connect() as conn:
            merge = SimpleNamespace(conn=conn, data_fields={"id": Integer()}, total_row_count=1, chunk_size=1,
                                    temp_table=Table("staging", MetaData(), Column("id", Integer)))
            observed = {}

            def fake_copy(*args):
                observed["sqlalchemy_transaction_at_copy"] = conn.in_transaction()
                raise RuntimeError("SIMULATED_COPY_ERROR")

            with patch.object(_bulk_load, "_csv_by_arrow", return_value=b'"1"\n'), \
                 patch.object(_bulk_load, "_has_pyarrow", return_value=True), \
                 patch.object(_bulk_load, "_copy", side_effect=fake_copy):
                try:
                    _bulk_load._postgres_copy(merge)
                except RuntimeError as exc:
                    observed["error"] = str(exc)
            return observed
    finally:
        engine.dispose()


def parity_helper_missing_rows():
    # Load only the helper so importing test_all does not open its sample DB.
    tree = ast.parse((Path(__file__).resolve().parents[1] / "tests/test_all.py").read_text())
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                    and node.name == "_assert_same_rows")
    first = [{"Shop": "S", "Product": str(i)} for i in range(3)]
    observed = {}
    for count in (0, 2, 4):
        second = first[:count] if count <= 3 else first + [{"Shop": "S", "Product": "extra"}]
        namespace = {
            "_stored_as_text": lambda engine, name, table: first if table == "Facts" else second,
            "_reflect_facts": lambda engine: SimpleNamespace(c=[]),
        }
        exec(compile(ast.Module(body=[function], type_ignores=[]), "_assert_same_rows", "exec"), namespace)
        try:
            namespace["_assert_same_rows"](None, "sqlite", "first", "second")
        except AssertionError:
            observed[str(count)] = "rejected"
        else:
            observed[str(count)] = "accepted"
    return observed


def main():
    logging.disable(logging.CRITICAL)
    result = {"environment": {"python": platform.python_version(), **{
        name: importlib.metadata.version(name)
        for name in ("SQLAlchemy", "pandas", "polars", "pyarrow", "duckdb", "psycopg", "psycopg2", "pg8000")}}}
    probes = {
        "DB-06-unique": unique_transition,
        "DB-07-generated": generated_column,
        "DB-08-later-field": lambda: input_shape([{"id": 1, "a": "first"},
                                                   {"id": 2, "a": "second", "b": "lost"}]),
        "DB-09-duplicate-label": lambda: input_shape(pd.DataFrame([[1, "first", "second"]],
                                                                  columns=["id", "value", "value"])),
        "DB-11-nullpool-create": nullpool_create,
        "DB-12-cleanup": cleanup_failure,
        "N-01-N-05-mark": mark_contract,
        "COPY-transaction-mock": copy_transaction_tracking,
        "COPY-JSON-local": json_csv,
        "T-04-parity-helper-second-row-count": parity_helper_missing_rows,
    }
    for name, probe in probes.items():
        try:
            result[name] = probe()
        except Exception as exc:
            result[name] = {"probe_error": error_info(exc)}
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
