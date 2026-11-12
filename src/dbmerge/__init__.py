from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("dbmerge")
except PackageNotFoundError:
    __version__ = "dev"

from .dbmerge import dbmerge, drop_table_if_exists, format_ms, mergeResult
from .dbmerge import IncorrectParameter, IncorrectDataError, NoKeyError, TableNotFoundError, TempTableAlreadyExists
from .tracking import (MergeTracker, MergeRecord, Database, database_of, TRACKING_TABLE,
                       HEARTBEAT_PERIOD, HEARTBEAT_TTL, TRACKING_UNSUPPORTED_DIALECTS)
from .lease import Lease
from .mart import MartLoop, MartContext, Handler, as_handler

__all__ = ["dbmerge", "drop_table_if_exists", "format_ms", "mergeResult", "__version__",
           "IncorrectParameter", "IncorrectDataError", "NoKeyError", "TableNotFoundError", "TempTableAlreadyExists",
           "MergeTracker", "MergeRecord", "Database", "database_of", "TRACKING_TABLE",
           "HEARTBEAT_PERIOD", "HEARTBEAT_TTL", "TRACKING_UNSUPPORTED_DIALECTS",
           "Lease", "MartLoop", "MartContext", "Handler", "as_handler"]