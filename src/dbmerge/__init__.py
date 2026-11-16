from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("dbmerge")
except PackageNotFoundError:
    __version__ = "dev"

from .dbmerge import dbmerge, drop_table_if_exists, format_ms, mergeResult, LeaseOptions
from .dbmerge import IncorrectParameter, IncorrectDataError, NoKeyError, TableNotFoundError, TempTableAlreadyExists
from .tracking import (MergeTracker, MergeRecord, TRACKING_TABLE,
                       HEARTBEAT_PERIOD, HEARTBEAT_TTL, TRACKING_UNSUPPORTED_DIALECTS)
from .lease import Lease, LeaseHandle, LeaseLost, LeaseBusy
from .mart import (MartLoop, MartContext, MartHandler, Handler,
                   request_stop, reset_stop, rebuild_requested, request_rebuild)

__all__ = ["dbmerge", "drop_table_if_exists", "format_ms", "mergeResult", "__version__",
           "IncorrectParameter", "IncorrectDataError", "NoKeyError", "TableNotFoundError", "TempTableAlreadyExists",
           "MergeTracker", "MergeRecord", "TRACKING_TABLE",
           "HEARTBEAT_PERIOD", "HEARTBEAT_TTL", "TRACKING_UNSUPPORTED_DIALECTS",
           "Lease", "LeaseOptions", "LeaseHandle", "LeaseLost", "LeaseBusy", "MartLoop", "MartContext", "MartHandler", "Handler",
           "request_stop", "reset_stop", "rebuild_requested", "request_rebuild"]