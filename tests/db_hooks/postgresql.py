"""PostgreSQL point fixtures for the shared suite."""

from dbmerge.postgresql import Point


def native_point_type():
    return Point(), True


def normalize_point(value):
    if isinstance(value, str):
        return tuple(map(float, value.strip('()').split(',')))
    return value