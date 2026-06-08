"""Asynchronous API for Weishaupt WebIF (Heat Pumps)."""

__version__ = "0.1.0"

from .api import WebifConnection
from .const import EXPECTED_COUNTS, ColoredFormatter, Info
from .exceptions import (
    ConnectionTimeoutError,
    McuResourceError,
    SessionExpiredError,
    WeishauptWebifError,
)

__all__ = [
    "EXPECTED_COUNTS",
    "ColoredFormatter",
    "ConnectionTimeoutError",
    "Info",
    "McuResourceError",
    "SessionExpiredError",
    "WebifConnection",
    "WeishauptWebifError",
    "__version__",
]
