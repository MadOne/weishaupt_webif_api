"""Tests for the HTML parsing logic."""

from collections.abc import Callable

from bs4 import BeautifulSoup
from weishaupt_webif_api import WebifConnection


def test_get_values_parsing(
    api: WebifConnection,
    load_fixture: Callable[[str], str],
) -> None:
    """Verify that the parser correctly extracts and cleans values from HTML."""
    html_content = load_fixture("info_statistik.html")
    soup = BeautifulSoup(html_content, "html.parser")

    # Find the Statistik column (usually the 3rd col-3 div in the batch)
    cols = soup.find_all("div", class_="col-3")
    assert len(cols) >= 3  # noqa: PLR2004

    values = api._get_values(cols[2])  # noqa: SLF001

    assert len(values) > 0
    for val in values.values():
        # Ensure units were stripped (no '°C' or 'KWh' should remain)
        assert not any(unit in str(val) for unit in ["°C", "KWh", "BAR"])
