import pytest
import os
from weishaupt_webif_api import WebifConnection


@pytest.fixture
def load_fixture():
    """
    Fixture that returns a helper function to load HTML
    files from the tests/fixtures directory.
    """

    def _load(filename):
        path = os.path.join(os.path.dirname(__file__), "fixtures", filename)
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    return _load


@pytest.fixture
async def api(tmp_path):
    conn = WebifConnection("10.10.1.225", "user", "pass", storage_path=str(tmp_path))
    yield conn
    await conn.close()
