"""Common fixtures for Weishaupt WebIF API tests."""

from collections.abc import AsyncGenerator, Callable
from pathlib import Path

import pytest
from weishaupt_webif_api import WebifConnection


@pytest.fixture
def load_fixture() -> Callable[[str], str]:
    """Fixture that returns a helper function to load HTML files.

    The helper loads files from the tests/fixtures directory.
    """

    def _load(filename: str) -> str:
        path = Path(__file__).parent / "fixtures" / filename
        with path.open("r", encoding="utf-8") as f:
            return f.read()

    return _load


@pytest.fixture
async def api(tmp_path: Path) -> AsyncGenerator[WebifConnection, None]:
    """Fixture for an active WebifConnection during testing."""
    conn = WebifConnection("10.10.1.225", "user", "pass", storage_path=tmp_path)
    yield conn
    await conn.close()
