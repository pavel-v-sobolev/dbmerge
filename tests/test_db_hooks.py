"""The shared suite's hook dispatch and Oracle test preparation, without a DB."""

from types import SimpleNamespace

import pytest
from sqlalchemy import Text

from db_hooks import dialect_hook


def test_unknown_dialect_and_missing_hook_keep_generic_path():
    assert dialect_hook('sqlite', 'prepare_engine') is None
    assert dialect_hook(None, 'prepare_parity_data') is None
    assert dialect_hook('oracle', 'missing_hook') is None


def test_oracle_hooks_are_loaded_on_demand():
    from db_hooks import oracle
    assert dialect_hook('oracle', 'prepare_engine') is oracle.prepare_engine
    assert dialect_hook('oracle', 'computed_column_options')() == {'persisted': False}


def test_oracle_parity_data_serializes_native_json_only():
    rows = [{'Data': {'a': [1, None]}, 'Note': ''}, {'Data': None, 'Note': None}]
    data_types = {'Note': Text()}
    dialect_hook('oracle', 'prepare_parity_data')(rows, data_types)
    assert rows == [{'Data': '{"a": [1, null]}', 'Note': ''}, {'Data': None, 'Note': None}]
    assert isinstance(data_types['Data'], Text)
    assert isinstance(data_types['Note'], Text)


def test_oracle_skips_only_the_native_json_scenario():
    hook = dialect_hook('oracle', 'prepare_test')
    hook(SimpleNamespace(node=SimpleNamespace(originalname='test_update_condition')))
    with pytest.raises(pytest.skip.Exception, match='native JSON'):
        hook(SimpleNamespace(node=SimpleNamespace(originalname='test_table_create_from_data_with_various_types')))


def test_oracle_prepares_real_schema_owners_and_quotas():
    statements = []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def scalar(self, statement, params):
            return params['owner'] == 'TARGET'

        def exec_driver_sql(self, statement):
            statements.append(statement)

        def commit(self):
            pass

    dialect_hook('oracle', 'prepare_engine')(SimpleNamespace(connect=Connection))
    creates = [s for s in statements if s.startswith('CREATE USER')]
    assert len(creates) == 2
    assert creates[0].startswith('CREATE USER SOURCE ')
    assert creates[1].startswith('CREATE USER TMP ')
    assert all('ACCOUNT LOCK' in s for s in creates)
    assert [s for s in statements if s.startswith('ALTER USER')] == [
        f'ALTER USER {owner} QUOTA 1G ON USERS' for owner in ('TARGET', 'SOURCE', 'TMP')]