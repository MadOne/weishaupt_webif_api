"""Main API module for Weishaupt WebIF communication."""

import asyncio
import json
import logging
import time
import types
from http.cookiejar import LWPCookieJar
from pathlib import Path
from typing import Any, Self, cast

import httpx
from bs4 import BeautifulSoup
from bs4.element import Tag

from .const import EXPECTED_COUNTS, INFO_HEADER, NAV_LABEL_TO_CATEGORY, UNITS, Info
from .exceptions import (
    ConnectionTimeoutError,
    McuResourceError,
    SessionExpiredError,
    WeishauptWebifError,
)

from .translation import WEBIF_CATEGORY_MAP, WEBIF_KEY_MAP

_LOGGER = logging.getLogger(__name__)

MAX_STATS_REQS = 100
HTTP_OK = 200


class WebifConnection:
    """A connection handler for the Weishaupt WebIF module.

    This class manages an asynchronous HTTP session, handles authentication
    via cookies, and provides methods to fetch and parse heat pump data.
    It includes built-in rate-limiting and error recovery logic to avoid
    overwhelming the heat pump microcontroller.
    """

    def __init__(  # noqa: PLR0913
        self,
        ip: str,
        user: str,
        password: str,
        *,
        request_delay: float = 10.0,
        cooldown_delay: float = 15.0,
        storage_path: str | Path | None = None,
    ) -> None:
        """Initialize the connection.

        :param ip: The IP address of the heat pump module.
        :param user: The username for login.
        :param password: The password for login.
        :param storage_path: Optional path for storing cookies and state files.
        :param request_delay: Breather delay between requests in seconds.
        :param cooldown_delay: Cooldown penalty after failure in seconds.
        """
        self._ip = ip
        self._username = user
        self._password = password
        self._token = None
        self._stack_codes: dict[str, str] = {}
        _LOGGER.debug("WebIF connection initialized with token")
        self._storage_path = Path(storage_path) if storage_path else Path.cwd()

        self._state_file = self._storage_path / "lwp_state.json"
        self._request_lock = asyncio.Lock()
        self._request_delay = request_delay
        self._cooldown_delay = cooldown_delay
        self._last_request_time = 0.0
        self._cooldown_until = 0.0
        self._base_url = f"http://{self._ip}"
        self._values: dict[str, dict[str, dict[str, str]]] = {}
        self._initialized = False
        self._stats = {
            "requests": 0,
            "successes": 0,
            "integrity_failures": 0,
            "timeouts": 0,
            "cooldowns": 0,
            "max_duration": 0.0,
        }

        self._headers = {
            "User-Agent": "Mozilla/5.0 Weishaupt-Webif-API/0.1.0",
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            ),
            "Accept-Language": "de-DE,de;q=0.9",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": self._base_url,
            "Referer": f"{self._base_url}/login.html",
        }

        self._client: httpx.AsyncClient | None = None
        self._cookies_loaded = False

        self._cookie_file = self._storage_path / "lwp_cookies.txt"
        self._cookie_jar = LWPCookieJar(filename=str(self._cookie_file))

    async def _load_state(self) -> None:
        """Load persisted timers and stack codes from the state file."""
        await asyncio.to_thread(
            self._storage_path.mkdir,
            parents=True,
            exist_ok=True,
        )

        def _load() -> dict[str, Any]:
            """Load state from the file."""
            try:
                with self._state_file.open(encoding="utf-8") as f:
                    return cast("dict[str, Any]", json.load(f))
            except (FileNotFoundError, json.JSONDecodeError):
                return {}

        try:
            state = await asyncio.to_thread(_load)

            expiry = state.get("cooldown_expiry", 0.0)
            remaining = expiry - time.time()

            if remaining > 0:
                self._cooldown_until = time.monotonic() + remaining
            else:
                self._cooldown_until = 0.0

            self._stack_codes = cast(
                "dict[str, str]",
                state.get("stack_codes", {}),
            )
            self._token = state.get("token", self._token)

        except OSError:
            self._cooldown_until = 0.0
            self._stack_codes = {}

        self._initialized = True

    async def _save_state(self) -> None:
        """Persist current timers and stack codes to the state file."""
        remaining = max(
            0,
            self._cooldown_until - time.monotonic(),
        )

        state_data = {
            "cooldown_expiry": (time.time() + remaining if remaining > 0 else 0.0),
            "stack_codes": self._stack_codes,
            "token": self._token,
        }

        def _save() -> None:
            with self._state_file.open("w", encoding="utf-8") as f:
                json.dump(state_data, f)

        await asyncio.to_thread(_save)

    async def _get_client(self) -> httpx.AsyncClient:
        """Return the initialized client and load session cookies."""
        if not self._cookies_loaded:
            try:
                await asyncio.to_thread(
                    self._cookie_jar.load,
                    ignore_discard=True,
                )
            except FileNotFoundError:
                _LOGGER.debug("Starting with empty cookie jar.")

            self._cookies_loaded = True

        if self._client is None:

            def _create_client() -> httpx.AsyncClient:
                return httpx.AsyncClient(
                    base_url=self._base_url,
                    headers=self._headers,
                    follow_redirects=True,
                    timeout=httpx.Timeout(60.0, connect=20.0),
                    limits=httpx.Limits(
                        max_connections=1,
                        max_keepalive_connections=1,
                        keepalive_expiry=120,
                    ),
                    cookies=self._cookie_jar,
                )

            self._client = await asyncio.to_thread(_create_client)

        return self._client

    async def __aenter__(self) -> Self:
        """Support async context manager."""
        if not self._initialized:
            await self._load_state()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        """Close client on exit."""
        await self.close()

    def _check_and_reset_stats(self, *, force: bool = False) -> None:
        """Check if request count reached threshold, log and reset stats."""
        req_count = self._stats["requests"]

        if req_count >= MAX_STATS_REQS or (force and req_count > 0):
            period = "final" if force else f"last {MAX_STATS_REQS} requests"

            _LOGGER.info(
                "Communication Statistics (%s): %s",
                period,
                self._stats,
            )

            self._stats.update(
                {
                    "requests": 0,
                    "successes": 0,
                    "integrity_failures": 0,
                    "timeouts": 0,
                    "cooldowns": 0,
                    "max_duration": 0.0,
                },
            )

    @property
    def stats(self) -> dict[str, int | float]:
        """Return communication statistics."""
        return self._stats

    async def close(self) -> None:
        """Gracefully close the underlying HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

        self._client = None
        self._cookies_loaded = False

    async def _request(
        self,
        method: str,
        url: str,
        **kwargs: Any,  # noqa: ANN401
    ) -> httpx.Response:
        """Perform a serialized HTTP request."""
        async with self._request_lock:
            if not self._initialized:
                await self._load_state()

            now = time.monotonic()

            if now < self._cooldown_until:
                self._stats["cooldowns"] += 1
                wait_time = self._cooldown_until - now

                _LOGGER.warning(
                    "In COOLDOWN period. Waiting %.1fs",
                    wait_time,
                )

                await asyncio.sleep(wait_time)
                now = time.monotonic()

            elapsed = now - self._last_request_time

            if elapsed < self._request_delay:
                time_to_wait = self._request_delay - elapsed

                _LOGGER.debug(
                    "Breather delay: waiting %.1fs",
                    time_to_wait,
                )

                await asyncio.sleep(time_to_wait)
                now = time.monotonic()

            return await self._do_request(
                method,
                url,
                **kwargs,
            )

    async def _do_request(
        self,
        method: str,
        url: str,
        **kwargs: Any,  # noqa: ANN401
    ) -> httpx.Response:
        """Execute an HTTP request and handle session expiration."""
        self._check_and_reset_stats()

        client = await self._get_client()
        self._stats["requests"] += 1
        start_time = time.monotonic()

        try:
            response = await client.request(
                method,
                url,
                **kwargs,
            )

        except httpx.ConnectTimeout as err:
            self._stats["timeouts"] += 1
            self._last_request_time = time.monotonic()
            self._cooldown_until = self._last_request_time + self._cooldown_delay
            await self._save_state()

            msg = f"Connect timeout to {url}"
            raise ConnectionTimeoutError(msg) from err

        except httpx.ConnectError as err:
            self._last_request_time = time.monotonic()
            self._cooldown_until = self._last_request_time + self._cooldown_delay
            await self._save_state()

            msg = f"Connection refused at {url}"
            raise WeishauptWebifError(msg) from err

        except httpx.ReadTimeout as err:
            self._stats["timeouts"] += 1
            self._last_request_time = time.monotonic()
            self._cooldown_until = self._last_request_time + self._cooldown_delay
            await self._save_state()

            msg = f"Read timeout from {url}"
            raise ConnectionTimeoutError(msg) from err

        except Exception as err:
            if isinstance(err, WeishauptWebifError):
                raise

            msg = f"Unexpected error: {err}"
            raise WeishauptWebifError(msg) from err

        else:
            duration = time.monotonic() - start_time

            self._stats["max_duration"] = max(
                self._stats["max_duration"],
                duration,
            )
            self._last_request_time = time.monotonic()

            await self._save_state()

            if "login.html" in str(response.url) or 'name="pass"' in response.text:
                return await self._handle_expired_session(
                    method,
                    url,
                    **kwargs,
                )

            _LOGGER.debug(
                "WEBIF %s %s -> %s",
                method,
                response.url,
                response.status_code,
            )

            return response

    async def _handle_expired_session(
        self,
        method: str,
        url: str,
        **kwargs: Any,  # noqa: ANN401
    ) -> httpx.Response:
        """Handle re-authentication when session expires."""
        if url != "/login.html":
            _LOGGER.info("Session expired. Re-authenticating...")

            await self._login()

            await asyncio.sleep(self._request_delay)

            self._stats["requests"] += 1
            start_retry = time.monotonic()

            client = await self._get_client()

            response = await client.request(
                method,
                url,
                **kwargs,
            )

            duration_retry = time.monotonic() - start_retry

            self._stats["max_duration"] = max(
                self._stats["max_duration"],
                duration_retry,
            )
            self._last_request_time = time.monotonic()

            await self._save_state()

            _LOGGER.debug(
                "WEBIF %s %s -> %s (retry)",
                method,
                response.url,
                response.status_code,
            )

            return response

        msg = "Authentication failed: Redirected to login."
        raise SessionExpiredError(msg)

    async def _login(self) -> None:
        """Perform the authentication process."""
        client = await self._get_client()

        _LOGGER.info("Attempting login to Weishaupt WebIF...")

        self._stats["requests"] += 1
        start_time = time.monotonic()

        try:
            response = await client.post(
                "/login.html",
                data={
                    "user": self._username,
                    "pass": self._password,
                },
            )

            end_time = time.monotonic()

            self._last_request_time = end_time

            self._stats["max_duration"] = max(
                self._stats["max_duration"],
                end_time - start_time,
            )

            await self._save_state()

        except httpx.HTTPError as err:
            msg = f"HTTP error during login: {err}"
            raise WeishauptWebifError(msg) from err

        final_url = str(response.url)

        if "wrongpassword" in final_url:
            msg = "Authentication failed: Invalid credentials."
            raise WeishauptWebifError(msg)

        if "nocon" in final_url:
            msg = "MCU database connection failure."
            raise McuResourceError(msg)

        if "home.html" in final_url:
            self._stats["successes"] += 1
            _LOGGER.info("Successful Login!")

            if self._client and isinstance(
                self._client.cookies.jar,
                LWPCookieJar,
            ):
                await asyncio.to_thread(
                    self._client.cookies.jar.save,
                    ignore_discard=True,
                    ignore_expires=True,
                )

        else:
            msg = "Login failed: Unexpected response."
            raise WeishauptWebifError(msg)

    async def _autodetect_token(self) -> str | None:
        """Fetch the settings_export overview page and parse the token."""
        _LOGGER.debug("Attempting to autodetect WebIF token...")

        response = await self._request(
            "GET",
            "/settings_export.html",
        )

        if response.status_code != HTTP_OK:
            _LOGGER.debug(
                "Could not reach /settings_export.html for autodetection",
            )
            return None

        soup = BeautifulSoup(
            markup=response.text,
            features="html.parser",
        )

        for link in soup.find_all("a"):
            href = str(link.get("href", ""))

            if "settings_export.html?stack=" not in href:
                continue

            stack = href.split("stack=", 1)[-1].strip()

            if len(stack) >= 38:
                token = stack[22:26]

                self._stats["successes"] += 1

                _LOGGER.info(
                    "Autodetected WebIF token: '%s'",
                    token,
                )

                return token

        _LOGGER.warning(
            "Could not autodetect WebIF token from /settings_export.html",
        )

        return None

    async def _discover_stack_codes(self) -> dict[str, str]:
        """Discover this device's per-category second-level stack codes."""
        _LOGGER.debug("Discovering WebIF stack codes")

        info_header = INFO_HEADER.format(
            token=self._token,
        )

        response = await self._request(
            "GET",
            f"/settings_export.html?stack={info_header}",
        )

        soup = BeautifulSoup(
            markup=response.text,
            features="html.parser",
        )

        codes: dict[str, str] = {}

        for link in soup.find_all("a"):
            if not isinstance(link, Tag):
                continue

            h5 = link.find("h5")
            href = str(link.get("href", ""))

            if h5 is None or "stack=" not in href or "," not in href:
                continue

            category = NAV_LABEL_TO_CATEGORY.get(
                h5.text.strip(),
            )

            if category is None:
                continue

            codes[category] = href.rsplit(
                ",",
                maxsplit=1,
            )[-1].strip()

        _LOGGER.debug(
            "Discovered WebIF stack codes (token '%s'): %s",
            self._token,
            codes,
        )

        if not codes:
            _LOGGER.warning(
                "Could not discover any category codes from the Info menu. "
                "The token '%s' is likely wrong for this device, or login failed.",
                self._token,
            )
        else:
            self._stats["successes"] += 1

        return codes

    async def update_all(
        self,
        categories: list[str] | None = None,
    ) -> dict[str, dict[str, str]]:
        """Fetch and update data categories from the heat pump.

        Categories use the normalized API names, for example
        ``heat_pump`` or ``statistics``.
        """
        if not self._initialized:
            await self._load_state()

        if not self._token:
            detected_token = await self._autodetect_token()

            if detected_token:
                self._token = detected_token
                await self._save_state()

        requested = self._validate_categories(categories)

        info_header = INFO_HEADER.format(
            token=self._token,
        )

        self._values.setdefault("Info", {})

        try:
            if not self._stack_codes:
                self._stack_codes = await self._discover_stack_codes()

                if self._stack_codes:
                    await self._save_state()

            for category in requested:
                section_data = await self._fetch_category_data(
                    category,
                    info_header,
                )

                if section_data:
                    normalized_category = normalize_webif_category(
                        category,
                    )

                    self._values["Info"][normalized_category] = section_data
                    self._stats["successes"] += 1

            _LOGGER.debug(
                "WebIF update cycle done (token='%s').",
                self._token,
            )

            await self._postprocess_values()

        except Exception as err:
            _LOGGER.warning(
                "WebIF update failed: %s. Resetting connection.",
                err,
            )
            await self.close()
            raise

        except asyncio.CancelledError:
            _LOGGER.debug(
                "WebIF update task cancelled. Cleaning up connection.",
            )
            await self.close()
            raise

        return self._values["Info"]

    async def _fetch_category_data(
        self,
        category: str,
        info_header: str,
    ) -> dict[str, str] | None:
        """Fetch and parse HTML data for a single WebIF category."""
        raw_category = denormalize_webif_category(category)

        code = self._stack_codes.get(raw_category)

        url = f"/settings_export.html?stack={info_header},{code}"

        response = await self._request(
            "GET",
            url,
        )

        if response.status_code != HTTP_OK:
            _LOGGER.warning(
                "WebIF returned HTTP %s for '%s'; skipping this cycle",
                response.status_code,
                category,
            )
            return None

        soup = BeautifulSoup(
            markup=response.text,
            features="html.parser",
        )

        cols = soup.find_all(
            "div",
            class_="col-3",
        )

        if len(cols) < 3:
            self._stats["integrity_failures"] += 1

            _LOGGER.warning(
                "Incomplete WebIF page for '%s': got %d column(s), "
                "expected at least 3. This just happens sometimes.",
                category,
                len(cols),
            )

            return None

        section_data = self._get_values(cols[2])
        found = len(section_data)
        expected = EXPECTED_COUNTS.get(raw_category, 0)

        if found == 0:
            self._stats["integrity_failures"] += 1

            _LOGGER.warning(
                "No values parsed for '%s' (token '%s'); skipping",
                category,
                self._token,
            )

            return None

        if expected and found != expected:
            _LOGGER.debug(
                "Section [%s]: %d entries (reference device has %d); "
                "keeping all parsed values",
                category,
                found,
                expected,
            )

        return section_data

    async def update_all_mock(
        self,
        categories: list[str] | None = None,
    ) -> dict[str, dict[str, str]]:
        """Return realistic data from a provided sample without polling.

        Categories use the normalized API names.
        """
        requested = self._validate_categories(categories)

        _LOGGER.debug(
            "Returning static mock data for development.",
        )

        mock_data = self._get_mock_data()
        self._values.setdefault("Info", {})

        for category in requested:
            raw_category = denormalize_webif_category(category)
            raw_section_data = mock_data.get(raw_category, {})
            section_data = self._normalize_values(raw_section_data)
            normalized_category = normalize_webif_category(raw_category)
            self._values["Info"][normalized_category] = section_data

        await self._postprocess_values()
        return self._values["Info"]

    def _validate_categories(
        self,
        categories: list[str] | None,
    ) -> list[str]:
        """Validate requested normalized API categories."""
        valid_categories = [normalize_webif_category(category) for category in Info]
        requested = categories if categories is not None else valid_categories
        for category in requested:
            if category not in valid_categories:
                msg = (
                    f"Invalid category '{category}'. Valid options: {valid_categories}"
                )
                raise ValueError(msg)
        return requested

    def _get_mock_data(self) -> dict[str, dict[str, str]]:
        """Return the real-world raw WebIF data sample."""
        return {
            "Heizkreis": {
                "Außentemperatur": "24.5",
                "AT Mittelwert": "25.0",
                "AT Langzeitwert": "25.5",
                "Raumsolltemperatur": "16.0",
                "Vorlaufsolltemperatur": "--",
                "Vorlauftemperatur": "62.0",
            },
            "Heizkreis1": {
                "Außentemperatur": "24.5",
                "AT Mittelwert": "25.0",
                "AT Langzeitwert": "25.5",
                "Raumsolltemperatur": "16.0",
                "Vorlaufsolltemperatur": "--",
                "Vorlauftemperatur": "62.0",
            },
            "Heizkreis2": {
                "Außentemperatur": "24.5",
                "AT Mittelwert": "25.0",
                "AT Langzeitwert": "25.5",
                "Raumsolltemperatur": "16.0",
                "Vorlaufsolltemperatur": "--",
                "Vorlauftemperatur": "62.0",
            },
            "Heizkreis3": {
                "Außentemperatur": "24.5",
                "AT Mittelwert": "25.0",
                "AT Langzeitwert": "25.5",
                "Raumsolltemperatur": "16.0",
                "Vorlaufsolltemperatur": "--",
                "Vorlauftemperatur": "62.0",
            },
            "Heizkreis4": {
                "Außentemperatur": "24.5",
                "AT Mittelwert": "25.0",
                "AT Langzeitwert": "25.5",
                "Raumsolltemperatur": "16.0",
                "Vorlaufsolltemperatur": "--",
                "Vorlauftemperatur": "62.0",
            },
            "Heizkreis5": {
                "Außentemperatur": "24.5",
                "AT Mittelwert": "25.0",
                "AT Langzeitwert": "25.5",
                "Raumsolltemperatur": "16.0",
                "Vorlaufsolltemperatur": "--",
                "Vorlauftemperatur": "62.0",
            },
            "Waermepumpe": {
                "Betrieb": "PV Optimierung",
                "Störmeldung": "--",
                "Warmwassertemperatur": "53.0",
                "Leistungsanforderung": "100",
                "Solltemperatur": "59.5",
                "Anforderung": "4.3",
                "Schaltdifferenz dynamisch": "5.0",
                "Vorlauftemperatur": "63.0",
                "Rücklauftemperatur": "57.5",
                "Drehzahl Pumpe M1": "80",
                "Volumenstrom": "1.7",
                "Stellung Umschaltventil": "Warmwasser",
                "Version WWP-SG": "V3.0",
                "Version WWP-EC WBB": "V5.3",
                "Soll Leistung": "9.9",
                "Ist Leistung": "9.6",
                "Expansionsventil AG Eintr": "46.0",
                "Luftansaugtemperatur": "24.5",
                "Wärmetauscher AG Austrit": "15.0",
                "Verdichtersauggastemp.": "23.0",
                "EVI Sauggastemperatur": "57.5",
                "Kältemittel IG Austritt": "54.0",
                "Ölsumpftemperatur": "41.5",
                "Druckgastemperatur": "95.0",
                "Niederdruck": "10.3",
                "Verdampfungstemperatur": "11.5",
                "Hochdruck": "39.1",
                "Kondensationstemperatur": "62.0",
                "Mitteldruck": "20.8",
                "Sättigungstemperatur EVI": "36.0",
                "Überhitzung Heizen": "3.0",
                "Öffnungsgrad EXV Heizen": "22",
                "Überhitzung Verdichter": "11.0",
                "Öffnungsgrad EXV Kühlen": "0",
                "Überhitzung EVI": "21.5",
                "Öffnungsgrad EVI": "48",
                "Betriebsstd. Verdichter": "7704",
                "Schaltspiele Verdichter": "3712",
                "Schaltspiele Abtauen": "1490",
                "Verdichter": "5013",
                "Außengerät Variante": "RMHA-10",
            },
            "2WEZ": {
                "Status": "0",
                "Status E-Heizung 1": "Aus",
                "Status E-Heizung 2": "Aus",
                "Betriebsstunden E1": "106",
                "Betriebsstunden E2": "71",
                "Schaltspiele E1": "58",
                "Schaltspiele E2": "20",
            },
            "Statistik": {
                "th. Energie Heizen Tag": "1.630",
                "th. Energie WW Tag": "5.498",
                "th. Energie gesamt Tag": "7.129",
                "elektrische Energie Tag": "1.882",
                "th. Energie Heizen Monat": "3.832",
                "th. Energie WW Monat": "42.393",
                "th. Energie gesamt Monat": "46.226",
                "elektrische Energie Monat": "14.420",
                "th. Energie Heizen Jahr": "8474.162",
                "th. Energie WW Jahr": "1594.194",
                "th. Energie gesamt Jahr": "10068.356",
                "elektrische Energie Jahr": "3157.207",
            },
        }

    def _get_values(self, soup: Tag) -> dict[str, str]:
        """Parse and normalize parameter names and values from HTML."""
        soup_links = soup.find_all(
            name="div",
            class_="nav-link browseobj",
        )

        values: dict[str, str] = {}

        for item in soup_links:
            if not isinstance(item, Tag):
                continue

            h5_tag = item.find("h5")

            if not h5_tag or not h5_tag.text:
                continue

            raw_key = h5_tag.text.strip()
            key = normalize_webif_key(raw_key)

            value = "".join(
                item.find_all(
                    string=True,
                    recursive=False,
                ),
            )

            for unit in UNITS:
                value = value.replace(
                    unit,
                    "",
                ).strip()

            values[key] = value

        return values

    def _normalize_values(
        self,
        values: dict[str, str],
    ) -> dict[str, str | None]:
        """Normalize raw WebIF parameter names and values."""
        result: dict[str, str | None] = {}

        for key, value in values.items():
            try:
                normalized_key = normalize_webif_key(key)
            except WeishauptWebifError:
                _LOGGER.exception("Unknown WebIF mock key: %r", key)
                raise

            result[normalized_key] = None if value == "--" else value

        return result

    async def _postprocess_values(self) -> None:
        """Safely clean up normalized values after fetching."""
        _LOGGER.debug(
            "Starting post processing of values",
        )

        info = self._values.get(
            "Info",
            {},
        )

        wp_info = info.get("heat_pump")

        if not wp_info:
            _LOGGER.debug(
                "Category 'heat_pump' not available; skipping post-processing.",
            )
            return

        if wp_info.get("actual_power") == "Aus":
            _LOGGER.debug(
                "Setting actual_power to 0",
            )
            wp_info["actual_power"] = "0"

        if wp_info.get("target_power") == "Aus":
            _LOGGER.debug(
                "Setting target_power to 0",
            )
            wp_info["target_power"] = "0"

        if wp_info.get("request") == "--":
            _LOGGER.debug(
                "Setting request to 0",
            )
            wp_info["request"] = "0"

    def get_value(
        self,
        category: str,
        key: str,
    ) -> str | None:
        """Return a value using normalized API category and key.

        :param category: Normalized category, e.g. ``heat_pump``.
        :param key: Normalized parameter key, e.g.
            ``flow_temperature``.
        :return: The stored value, or ``None`` if unavailable.
        """
        return (
            self._values.get(
                "Info",
                {},
            )
            .get(
                category,
                {},
            )
            .get(key)
        )


def normalize_webif_category(category: str) -> str:
    """Convert a raw WebIF category to its normalized API name."""
    try:
        return WEBIF_CATEGORY_MAP[category]
    except KeyError as err:
        raise WeishauptWebifError(
            f"Unknown WebIF category: {category!r}",
        ) from err


def denormalize_webif_category(category: str) -> str:
    """Convert a normalized API category to its raw WebIF name."""
    for raw_category, normalized_category in WEBIF_CATEGORY_MAP.items():
        if normalized_category == category:
            return raw_category

    raise WeishauptWebifError(
        f"Unknown normalized WebIF category: {category!r}",
    )


def normalize_webif_key(key: str) -> str:
    """Convert a raw WebIF parameter name to its normalized API name."""
    try:
        return WEBIF_KEY_MAP[key]
    except KeyError as err:
        raise WeishauptWebifError(
            f"Unknown WebIF key: {key!r}",
        ) from err
