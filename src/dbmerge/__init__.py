from importlib.metadata import version, PackageNotFoundError
from warnings import filterwarnings as _filterwarnings

# ClickHouse Connect can warn while SQLAlchemy creates the engine, before DBMerge
# can validate its actual dialect. Ignore only the bare-alias notice, not other warnings.
_filterwarnings(
    'ignore',
    message=(r"^The 'clickhouse://' URL remains registered to another SQLAlchemy dialect\. "
             r"ClickHouse Connect is available through 'clickhousedb://' or 'clickhousedb\+connect://'\.$"),
    category=UserWarning,
)

try:
    __version__ = version("dbmerge")
except PackageNotFoundError:
    __version__ = "dev"

from .dbmerge import DBMerge, dbmerge, drop_table_if_exists, format_ms, mergeResult, LeaseOptions
from .dbmerge import IncorrectParameter, IncorrectDataError, NoKeyError, TableNotFoundError, TempTableAlreadyExists
from .tracking import (MergeTracker, MergeRecord, TRACKING_TABLE,
                       HEARTBEAT_PERIOD, HEARTBEAT_TTL, TRACKING_UNSUPPORTED_DIALECTS)
from .lease import Lease, LeaseHandle, LeaseLost, LeaseBusy
from .mart import (MartLoop, MartContext, MartHandler, Handler,
                   request_stop, reset_stop, rebuild_requested, request_rebuild)

__all__ = ["DBMerge", "dbmerge", "drop_table_if_exists", "format_ms", "mergeResult", "__version__",
           "IncorrectParameter", "IncorrectDataError", "NoKeyError", "TableNotFoundError", "TempTableAlreadyExists",
           "MergeTracker", "MergeRecord", "TRACKING_TABLE",
           "HEARTBEAT_PERIOD", "HEARTBEAT_TTL", "TRACKING_UNSUPPORTED_DIALECTS",
           "Lease", "LeaseOptions", "LeaseHandle", "LeaseLost", "LeaseBusy", "MartLoop", "MartContext", "MartHandler", "Handler",
           "request_stop", "reset_stop", "rebuild_requested", "request_rebuild"]