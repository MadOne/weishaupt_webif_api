"""Asynchronous API for Weishaupt WebIF (Heat Pumps)."""

__version__ = "0.1.0"

from .api import WebifConnection
from .const import EXPECTED_COUNTS, Info, ColoredFormatter
from .exceptions import (
    ConnectionTimeoutError,
    McuResourceError,
    SessionExpiredError,
    WeishauptWebifError,
)

__all__ = [
    "__version__",
    "WebifConnection",
    "ColoredFormatter",
    "WeishauptWebifError",
    "McuResourceError",
    "SessionExpiredError",
    "ConnectionTimeoutError",
    "Info",
    "EXPECTED_COUNTS",
]
