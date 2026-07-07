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

# The 4-hex-digit token in these stack strings is device specific (it appears
# in the WebIF page's own navigation links / address bar). It is substituted at
# runtime from the configured token via str.format(token=...), so it must not
# be hardcoded here.
INFO_HEADER = "0C00000100000000008000{token}010002000301"
#              0C00000100000000008000 F9AF  010002000301

Info = {
    "Heizkreis": "0C000C1900000000000000{token}020003000401",
    "Waermepumpe": "0C000C2200000000000000{token}020003000401",
    "2WEZ": "0C000C2300000000000000{token}020003000401",
    "Statistik": "0C000C2700000000000000{token}020003000401",
}

# The second-level stack codes above differ between WEM models, so they are
# discovered at runtime from the Info menu's own navigation links. This maps the
# German menu label (as shown in the WebIF) to the internal category key.
NAV_LABEL_TO_CATEGORY = {
    "Heizkreis": "Heizkreis",
    "Wärmepumpe": "Waermepumpe",
    "2. WEZ": "2WEZ",
    "Statistik": "Statistik",
}

EXPECTED_COUNTS = {
    "Heizkreis": 6,
    "Waermepumpe": 41,
    "2WEZ": 7,
    "Statistik": 12,
}
