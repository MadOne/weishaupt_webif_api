import asyncio
import json
import logging
import os
import time
from typing import Any
from http.cookiejar import LWPCookieJar

from bs4 import BeautifulSoup
from bs4.element import Tag
import httpx

from .const import UNITS, Info, EXPECTED_COUNTS
from .exceptions import (
    WeishauptWebifError,
    McuResourceError,
    SessionExpiredError,
    ConnectionTimeoutError,
)

_LOGGER = logging.getLogger(__name__)


class WebifConnection:
    """
    A connection handler for the Weishaupt WebIF module.

    This class manages an asynchronous HTTP session, handles authentication
    via cookies, and provides methods to fetch and parse heat pump data.
    It includes built-in rate-limiting and error recovery logic to avoid
    overwhelming the heat pump microcontroller.
    """

    def __init__(self, ip, user, password, storage_path: str | None = None) -> None:
        """
        Initialize the connection.

        :param ip: The IP address of the heat pump module.
        :param user: The username for login.
        :param password: The password for login.
        :param storage_path: Optional path for storing cookies and state files.
        """
        self._ip = ip
        self._username = user
        self._password = password
        self._client: httpx.AsyncClient | None = None
        self._storage_path = storage_path or os.getcwd()
        if not os.path.exists(self._storage_path):
            os.makedirs(self._storage_path, exist_ok=True)
        self._state_file = os.path.join(self._storage_path, "lwp_state.json")
        self._request_lock = asyncio.Lock()
        self._request_delay = 60.0
        self._last_request_time = 0.0  # Monotonic
        self._cooldown_until = 0.0  # Monotonic
        self._base_url = f"http://{self._ip}"
        self._values: dict[str, Any] = {}
        self._stats = {
            "requests": 0,
            "successes": 0,
            "integrity_failures": 0,
            "timeouts": 0,
            "cooldowns": 0,
            "max_duration": 0.0,
        }

        self._load_state()

    def _load_state(self) -> None:
        """Load persisted timers from the state file."""
        try:
            with open(self._state_file, "r", encoding="utf-8") as f:
                state = json.load(f)
                # Recover cooldown relative to current wall clock
                expiry = state.get("cooldown_expiry", 0.0)
                remaining = expiry - time.time()
                if remaining > 0:
                    self._cooldown_until = time.monotonic() + remaining
                else:
                    self._cooldown_until = 0.0
        except (FileNotFoundError, json.JSONDecodeError):
            self._cooldown_until = 0.0

    def _save_state(self) -> None:
        """Persist current timers to the state file."""
        # Calculate wall-clock expiry for persistence
        remaining = max(0, self._cooldown_until - time.monotonic())
        state = {"cooldown_expiry": time.time() + remaining if remaining > 0 else 0.0}
        with open(self._state_file, "w", encoding="utf-8") as f:
            json.dump(state, f)

    def _get_client(self) -> httpx.AsyncClient:
        """
        Initialize or return the existing httpx AsyncClient.

        Sets up default headers and loads cookies from the local lwp_cookies.txt file
        to maintain session state between script restarts.

        :return: An active httpx.AsyncClient instance.
        """
        if self._client is None:
            headers = {
                "User-Agent": "Mozilla/5.0 Weishaupt-Webif-API/0.1.0",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": self._base_url,
                "Referer": f"{self._base_url}/login.html",
                "Connection": "close",
            }
            cookie_file = os.path.join(self._storage_path, "lwp_cookies.txt")
            cookies = LWPCookieJar(filename=cookie_file)
            try:
                cookies.load(ignore_discard=True)
            except FileNotFoundError:
                _LOGGER.debug("Starting with empty cookie jar.")

            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                headers=headers,
                follow_redirects=True,
                timeout=httpx.Timeout(60.0, connect=20.0),
                limits=httpx.Limits(max_connections=1, max_keepalive_connections=0),
                cookies=cookies,
            )
        return self._client

    async def __aenter__(self):
        """Support async context manager."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close client on exit."""
        await self.close()

    def _check_and_reset_stats(self, force: bool = False) -> None:
        """Check if request count reached threshold, log and reset stats."""
        if self._stats["requests"] >= 100 or (force and self._stats["requests"] > 0):
            period = "final" if force else "last 100 requests"
            _LOGGER.info("Communication Statistics (%s): %s", period, self._stats)
            # Update in place to maintain reference integrity
            self._stats.update(
                {
                    "requests": 0,
                    "successes": 0,
                    "integrity_failures": 0,
                    "timeouts": 0,
                    "cooldowns": 0,
                    "max_duration": 0.0,
                }
            )

    @property
    def stats(self) -> dict[str, int | float]:
        """Return communication statistics."""
        return self._stats

    async def close(self) -> None:
        """Gracefully close the underlying HTTP client."""
        self._check_and_reset_stats(force=True)
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """
        Perform a serialized HTTP request.

        Ensures that only one request is active at a time and enforces cooldown
        or breather delays before execution.

        :param method: HTTP method (e.g., 'GET', 'POST').
        :param url: The relative or absolute URL to request.
        :return: The httpx.Response object.
        """
        async with self._request_lock:
            now = time.monotonic()
            if now < self._cooldown_until:
                self._stats["cooldowns"] += 1
                wait_time = self._cooldown_until - now
                _LOGGER.warning("In COOLDOWN period. Waiting %.1fs", wait_time)
                await asyncio.sleep(wait_time)
                now = time.monotonic()

            elapsed = now - self._last_request_time
            if elapsed < self._request_delay:
                time_to_wait = self._request_delay - elapsed
                _LOGGER.debug("Breather delay: waiting %.1fs", time_to_wait)
                await asyncio.sleep(time_to_wait)
                now = time.monotonic()

            return await self._do_request(method, url, **kwargs)

    async def _do_request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        """
        Execute an HTTP request and handle session expiration.

        If a request returns a login page, this method automatically attempts
        to re-authenticate and retries the original request.

        :param method: HTTP method.
        :param url: Target URL.
        :return: The httpx.Response object.
        """
        self._check_and_reset_stats()
        client = self._get_client()
        self._stats["requests"] += 1
        start_time = time.monotonic()

        try:
            response = await client.request(method, url, **kwargs)
            end_time = time.monotonic()
            duration = end_time - start_time
            self._stats["max_duration"] = max(self._stats["max_duration"], duration)

            self._last_request_time = end_time
            self._save_state()

            if "login.html" in str(response.url) or 'name="pass"' in response.text:
                if url != "/login.html":
                    _LOGGER.info("Session expired. Re-authenticating...")
                    await self._login()
                    # Retry original request after re-login
                    self._stats["requests"] += 1
                    start_retry = time.monotonic()
                    response = await client.request(method, url, **kwargs)
                    end_retry = time.monotonic()
                    duration_retry = end_retry - start_retry

                    self._stats["max_duration"] = max(
                        self._stats["max_duration"], duration_retry
                    )
                    self._last_request_time = end_retry
                    self._save_state()
                    _LOGGER.debug(
                        "WEBIF %s %s -> %s (retry)",
                        method,
                        response.url,
                        response.status_code,
                    )
                    return response
                raise SessionExpiredError("Authentication failed: Redirected to login.")

            _LOGGER.debug(
                "WEBIF %s %s -> %s", method, response.url, response.status_code
            )
            return response

        except httpx.ConnectTimeout as err:
            self._stats["timeouts"] += 1
            self._last_request_time = time.monotonic()
            self._cooldown_until = self._last_request_time + 300.0
            self._save_state()
            raise ConnectionTimeoutError(f"Connect timeout to {url}") from err
        except httpx.ConnectError as err:
            self._last_request_time = time.monotonic()
            self._cooldown_until = self._last_request_time + 300.0
            self._save_state()
            raise WeishauptWebifError(f"Connection refused at {url}") from err
        except httpx.ReadTimeout as err:
            self._stats["timeouts"] += 1
            self._last_request_time = time.monotonic()
            self._cooldown_until = self._last_request_time + 300.0
            self._save_state()
            raise ConnectionTimeoutError(f"Read timeout from {url}") from err
        except Exception as err:
            if isinstance(err, WeishauptWebifError):
                raise
            raise WeishauptWebifError(f"Unexpected error: {err}") from err

    async def _login(self) -> None:
        """
        Perform the authentication process.

        Sends a POST request with credentials and verifies the redirection URL.
        Saves valid session cookies to the disk upon success.
        """
        client = self._get_client()
        _LOGGER.info("Attempting login to Weishaupt WebIF...")

        # Explicit direct request to avoid recursion through _do_request
        self._stats["requests"] += 1
        start_time = time.monotonic()

        try:
            response = await client.post(
                "/login.html",
                data={"user": self._username, "pass": self._password},
            )
            end_time = time.monotonic()
            self._last_request_time = end_time
            self._stats["max_duration"] = max(
                self._stats["max_duration"], end_time - start_time
            )
            self._save_state()
        except httpx.HTTPError as err:
            # Errors during login are caught by the caller (_do_request or update_all)
            raise WeishauptWebifError(f"HTTP error during login: {err}") from err

        final_url = str(response.url)
        if "wrongpassword" in final_url:
            raise WeishauptWebifError("Authentication failed: Invalid credentials.")
        elif "nocon" in final_url:
            raise McuResourceError("MCU database connection failure.")
        elif "home.html" in final_url:
            _LOGGER.info("✅ Successful Login!")
            if self._client and isinstance(self._client.cookies.jar, LWPCookieJar):
                self._client.cookies.jar.save(ignore_discard=True, ignore_expires=True)
        else:
            raise WeishauptWebifError("Login failed: Unexpected response.")

    async def update_all(self, categories: list[str] | None = None) -> dict[str, Any]:
        """
        Fetch and update data categories from the heat pump.

        If categories is None, all available categories are updated.
        Requests are filtered and split into batches to minimize MCU stress.

        :param categories: List of categories to fetch (e.g., ['Statistik']).
        :return: A dictionary containing the updated data grouped by category.
        :raises ValueError: If an invalid category name is provided.
        :raises McuResourceError: If data integrity check fails.
        """
        valid_categories = list(Info.keys())
        requested = categories if categories is not None else valid_categories

        for cat in requested:
            if cat not in valid_categories:
                raise ValueError(
                    f"Invalid category '{cat}'. Valid options: {valid_categories}"
                )

        info_header = "0C00000100000000008000F9AF010002000301"
        self._values.setdefault("Info", {})

        # Respect original safe batching structure but filter for requested items
        safe_batches = [["Heizkreis", "2WEZ", "Statistik"], ["Waermepumpe"]]
        active_batches = []
        for batch in safe_batches:
            filtered = [c for c in batch if c in requested]
            if filtered:
                active_batches.append(filtered)

        for batch_cats in active_batches:
            stack_string = f"{info_header}," + ",".join([Info[c] for c in batch_cats])
            url = f"/settings_export.html?stack={stack_string}"
            try:
                response = await self._request("GET", url)
                if response.status_code != 200:
                    raise WeishauptWebifError(f"HTTP {response.status_code}")

                soup = BeautifulSoup(markup=response.text, features="html.parser")
                cols = soup.find_all("div", class_="col-3")
                if len(cols) < (2 + len(batch_cats)):
                    self._cooldown_until = time.monotonic() + 300.0
                    self._stats["integrity_failures"] += 1
                    self._save_state()
                    raise McuResourceError("Incomplete HTML: missing columns")

                for i, category in enumerate(batch_cats):
                    section_data = self._get_values(cols[i + 2])
                    found, expected = (
                        len(section_data),
                        EXPECTED_COUNTS.get(category, 0),
                    )
                    _LOGGER.debug(
                        "Section [%s]: %d/%d entries", category, found, expected
                    )
                    if found != expected or found == 0:
                        self._stats["integrity_failures"] += 1
                        self._cooldown_until = time.monotonic() + 300.0
                        self._save_state()
                        raise McuResourceError(f"Data integrity error in {category}")
                    self._values["Info"][category] = section_data

                self._stats["successes"] += 1
            except (httpx.HTTPError, WeishauptWebifError):
                raise

        _LOGGER.debug("Update cycle successful.")
        return self._values["Info"]

    def _get_values(self, soup: Tag) -> dict[str, Any]:
        """
        Parse parameter names and values from a specific HTML column.

        This method strips known units (like °C or KWh) from the values
        to return clean data.

        :param soup: A BeautifulSoup Tag representing an HTML 'col-3' div.
        :return: A dictionary of parameter names and their cleaned values.
        """
        soup_links = soup.find_all(name="div", class_="nav-link browseobj")
        values: dict[str, Any] = {}
        for item in soup_links:
            if not isinstance(item, Tag):
                continue
            h5_tag = item.find("h5")
            if h5_tag and h5_tag.text:
                name = h5_tag.text.strip()
                value = "".join(item.find_all(string=True, recursive=False))
                for unit in UNITS:
                    value = value.replace(unit, "").strip()
                values[name] = value
        return values
