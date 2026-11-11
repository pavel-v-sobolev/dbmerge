"""Oracle setup for test_all.py. Run only against a disposable test database.

The shared suite uses three real schemas, not aliases of the connecting user.
An administrative test connection creates missing schema owners. A custom URL
must have equivalent cross-schema DDL/DML permissions and pre-provisioned users,
or CREATE USER/ALTER USER permission for the setup below.
"""

import json
import uuid

import pytest
from sqlalchemy import Text, text


def prepare_engine(engine):
    with engine.connect() as conn:
        for name in ('target', 'source', 'tmp'):
            owner = name.upper()
            exists = conn.scalar(text('SELECT count(*) FROM all_users WHERE username = :owner'),
                                 {'owner': owner})
            if not exists:
                # No login is granted. Random credentials prevent a known-password account.
                password = 'Dm_' + uuid.uuid4().hex[:24]
                conn.exec_driver_sql(f'CREATE USER {owner} IDENTIFIED BY "{password}" '
                                     'DEFAULT TABLESPACE USERS ACCOUNT LOCK')
            # The shared source-table scenario loads over a million rows. These
            # fixed owners belong exclusively to this disposable test database.
            conn.exec_driver_sql(f'ALTER USER {owner} QUOTA 1G ON USERS')
        conn.commit()


def prepare_test(request):
    if request.node.originalname == 'test_table_create_from_data_with_various_types':
        pytest.skip('Oracle native JSON is not supported; this scenario infers a native JSON column')


def prepare_parity_data(data, data_types):
    # Exercise the same loader parity assertions with the documented Oracle
    # representation of structured values: explicit JSON text, not native JSON.
    for row in data:
        if row['Data'] is not None:
            row['Data'] = json.dumps(row['Data'])
    data_types['Data'] = Text()


def computed_column_options():
    return {'persisted': False}