"""ClickHouse point fixtures for the shared suite."""

def native_point_type():
    from clickhouse_connect.cc_sqlalchemy.datatypes.base import sqla_type_from_name

    return sqla_type_from_name('Point'), False


def point_source_query(engine, table):
    name = engine.dialect.identifier_preparer.format_table(table)
    return f'SELECT id, location FROM {name} FINAL'