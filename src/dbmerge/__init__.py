from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("dbmerge")
except PackageNotFoundError:
    __version__ = "dev"

from .dbmerge import dbmerge, drop_table_if_exists, format_ms, mergeResult
from .dbmerge import IncorrectParameter, IncorrectDataError, NoKeyError, TableNotFoundError, TempTableAlreadyExists

__all__ = ["dbmerge", "drop_table_if_exists", "format_ms", "mergeResult", "__version__",
           "IncorrectParameter", "IncorrectDataError", "NoKeyError", "TableNotFoundError", "TempTableAlreadyExists"]