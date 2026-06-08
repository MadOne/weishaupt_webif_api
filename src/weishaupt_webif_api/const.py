"""Constants and formatters for the Weishaupt WebIF API."""

import logging
from typing import ClassVar


class ColoredFormatter(logging.Formatter):
    """Custom logging formatter that applies ANSI color codes to output.

    Designed to improve visibility of log levels in terminal environments.
    """

    grey = "\x1b[38;21m"
    blue = "\x1b[34m"
    yellow = "\x1b[33m"
    red = "\x1b[31m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format_str = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

    FORMATS: ClassVar[dict[int, str]] = {
        logging.DEBUG: grey + format_str + reset,
        logging.INFO: blue + format_str + reset,
        logging.WARNING: yellow + format_str + reset,
        logging.ERROR: red + format_str + reset,
        logging.CRITICAL: bold_red + format_str + reset,
    }

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record with ANSI color codes."""
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


UNITS = ["m3/h", " KWh", " KW", " °C", " BAR", " rpm", " %", " K", " h"]

Info = {
    "Heizkreis": "0C000C1900000000000000F9AF020003000401",
    "Waermepumpe": "0C000C2200000000000000F9AF020003000401",
    "2WEZ": "0C000C2300000000000000F9AF020003000401",
    "Statistik": "0C000C2700000000000000F9AF020003000401",
}

EXPECTED_COUNTS = {
    "Heizkreis": 6,
    "Waermepumpe": 41,
    "2WEZ": 7,
    "Statistik": 12,
}
