"""Shared validation for intervals used by background workers."""

from math import isfinite

from .dbmerge import IncorrectParameter


def validate_interval(name, value, *, allow_zero=False):
    if (isinstance(value, bool) or not isinstance(value, (int, float))
            or not isfinite(value) or value < 0 or not allow_zero and value == 0):
        constraint = 'non-negative' if allow_zero else 'positive'
        raise IncorrectParameter(f'{name} must be a finite {constraint} number')