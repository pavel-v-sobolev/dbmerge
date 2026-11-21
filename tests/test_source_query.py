import uuid

import pytest
from sqlalchemy import Integer, bindparam, create_engine, event, literal, select, text, union_all

from conftest import engines, make_engine
from dbmerge import DBMerge, IncorrectDataError, IncorrectParameter, drop_table_if_exists


@pytest.mark.parametrize('engine_name', list(engines))
@pytest.mark.parametrize('query_kind', ['sql', 'select', 'text'])
def test_source_query_loads_and_updates(engine_name, query_kind):
    engine = make_engine(engine_name)
    dialect = engine.dialect.name
    schema = 'target' if dialect in ('mysql', 'mariadb') else None
    name = 'sq_' + uuid.uuid4().hex[:12]
    from_clause = ' FROM dual' if dialect == 'oracle' else ''

    def source(value):
        if query_kind == 'sql':
            return f'SELECT 1 AS id, {value} AS qty{from_clause};'
        if query_kind == 'text':
            return text(f'SELECT 1 AS id, :qty AS qty{from_clause}').bindparams(bindparam('qty', value, type_=Integer()))
        return union_all(select(literal(1).label('id'), literal(value).label('qty')),
                         select(literal(2).label('id'), literal(99).label('qty')))

    try:
        for qty in (10, 20):
            with DBMerge(engine, name, source_query=source(qty), schema=schema, key=['id']) as merge:
                result = merge.exec(source_condition=merge.source_table.c.id == 1)
            assert result.total_row_count == 1
            assert (result.inserted_row_count, result.updated_row_count) == ((1, 0) if qty == 10 else (0, 1))
        with engine.connect() as conn:
            statement = select(merge.table.c.id, merge.table.c.qty)
            if dialect == 'clickhousedb':
                from clickhouse_connect.cc_sqlalchemy.sql import final
                statement = final(statement, merge.table)
            assert conn.execute(statement).all() == [(1, 20)]
    finally:
        drop_table_if_exists(engine, name, schema=schema)


@pytest.mark.parametrize('query', ['', 'DELETE FROM items', text('UPDATE items SET id = 2'), 42])
def test_invalid_source_queries(query):
    with pytest.raises(IncorrectParameter, match='source_query.*SELECT'):
        DBMerge(create_engine('sqlite://'), 'items', source_query=query, key=['id'])


@pytest.mark.parametrize('kwargs', [{'data': []}, {'source_table_name': 'other'}, {'source_schema': 'main'}])
def test_source_query_conflicts(kwargs):
    with pytest.raises(IncorrectParameter, match='source_query|source_schema'):
        DBMerge(create_engine('sqlite://'), 'items', source_query='SELECT 1 AS id', key=['id'], **kwargs)


def test_empty_query_with_and_without_explicit_types():
    engine = create_engine('sqlite://')
    query = 'SELECT 1 AS id, NULL AS qty WHERE 0'
    try:
        with pytest.raises(IncorrectDataError, match='data_types'):
            DBMerge(engine, 'items', source_query=query, key=['id'])
        with DBMerge(engine, 'items', source_query=query, key=['id'],
                     data_types={'id': Integer(), 'qty': Integer()}) as merge:
            assert merge.exec().total_row_count == 0
        with engine.begin() as conn:
            conn.exec_driver_sql('INSERT INTO items (id, qty) VALUES (1, 2)')
        with DBMerge(engine, 'items', source_query=query, delete_mode='delete') as merge:
            result = merge.exec()
        assert result.total_row_count == 0
        assert result.deleted_row_count == 1
    finally:
        engine.dispose()


def test_sql_join_is_loaded_on_the_server_without_sampling_known_types():
    engine = create_engine('sqlite://')
    statements = []
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql('CREATE TABLE orders (id INTEGER PRIMARY KEY, item_id INTEGER)')
            conn.exec_driver_sql('CREATE TABLE prices (id INTEGER PRIMARY KEY, amount INTEGER)')
            conn.exec_driver_sql('INSERT INTO orders VALUES (1, 5), (2, 6)')
            conn.exec_driver_sql('INSERT INTO prices VALUES (5, 20), (6, 30)')

        @event.listens_for(engine, 'before_cursor_execute')
        def record_query(conn, cursor, sql, parameters, context, executemany):
            if 'JOIN prices' in sql:
                statements.append(sql)

        query = '''
            SELECT orders.id, prices.amount
            FROM orders JOIN prices ON orders.item_id = prices.id
        '''
        with DBMerge(engine, 'summary', source_query=query, key=['id'],
                     data_types={'id': Integer(), 'amount': Integer()}) as merge:
            result = merge.exec(source_condition=merge.source_table.c.amount > 20)
        assert result.total_row_count == result.inserted_row_count == 1
        assert len(statements) == 2  # Zero-row column discovery, then server-side INSERT ... SELECT.
        assert 'WHERE 0 = 1' in statements[0]
        assert statements[1].startswith('INSERT INTO tmp_')
        with engine.connect() as conn:
            assert conn.exec_driver_sql('SELECT id, amount FROM summary').all() == [(2, 30)]
    finally:
        engine.dispose()