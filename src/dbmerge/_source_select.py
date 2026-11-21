"""Validate the outer source SELECT without inspecting nested query scopes."""

from sqlalchemy.sql import COLLECT_CARTESIAN_PRODUCTS


def validate_source_select(statement, dialect=None):
    # Use SQLAlchemy's own FROM graph: it understands aliases, explicit JOINs,
    # WHERE links and correlation. Collect only: warning filters must not disable
    # the check, and unrelated cartesian products inside subqueries are not ours
    # to reject. A caller-supplied query may need its dialect to compile.
    compiled = statement.compile(dialect=dialect, linting=COLLECT_CARTESIAN_PRODUCTS)
    disconnected, _ = compiled.from_linter.lint()
    if disconnected:
        from .dbmerge import IncorrectParameter
        raise IncorrectParameter(
            'source_condition introduces disconnected FROM elements into the source SELECT. '
            'Use merge.source_table columns to filter source rows; use merge.table columns '
            'in delete_condition to filter destination rows, or explicitly link additional sources.')
