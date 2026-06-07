class WeishauptWebifError(Exception):
    """Base class for all library errors."""


class McuResourceError(WeishauptWebifError):
    """Raised when the MCU is under memory pressure or rendering fails."""


class SessionExpiredError(WeishauptWebifError):
    """Raised when the session cookie is no longer valid."""


class ConnectionTimeoutError(WeishauptWebifError):
    """Raised when a request to the web server times out."""
