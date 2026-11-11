"""Optional database-specific setup for the shared test suite.

Like the production dialect hooks, missing hooks leave the generic path intact.
Keep assertions and merge scenarios in test_all.py, not in these modules.
"""

from importlib import import_module


DIALECT_MODULES = {'oracle': 'oracle'}


def dialect_hook(dialect_name, name):
    module_name = DIALECT_MODULES.get(dialect_name)
    if module_name is None:
        return None
    return getattr(import_module(f'{__name__}.{module_name}'), name, None)