import asyncio
import os
import time
import re
import pytest
import respx
import httpx
from weishaupt_webif_api import WebifConnection, McuResourceError


@pytest.mark.asyncio
async def test_update_selective_success(api, load_fixture):
    """
    Test fetching only a specific category using an external HTML fixture.
    """
    mock_html = load_fixture("info_statistik.html")

    async with respx.mock:
        route = respx.get(url__regex=re.compile(r".*0C000C27.*")).mock(
            return_value=httpx.Response(200, text=mock_html)
        )

        data = await api.update_all(["Statistik"])

        assert route.called
        assert "Statistik" in data
        assert data["Statistik"]["Value 0"] == "0"
        assert "Waermepumpe" not in data


@pytest.mark.asyncio
async def test_update_all_batching(api):
    """Verify that update_all still splits requests into safe batches."""
    mock_h = '<html><div class="col-3"></div><div class="col-3"></div>'
    mock_batch1 = mock_h + '<div class="col-3"></div>' * 3 + "</html>"
    mock_batch2 = mock_h + '<div class="col-3"></div>' * 1 + "</html>"

    async with respx.mock:
        respx.get(url__regex=re.compile(r".*0C000C19.*")).mock(
            return_value=httpx.Response(200, text=mock_batch1)
        )
        respx.get(url__regex=re.compile(r".*0C000C22.*")).mock(
            return_value=httpx.Response(200, text=mock_batch2)
        )

        with pytest.raises(McuResourceError):
            await api.update_all()


@pytest.mark.asyncio
async def test_persistence_logic(tmp_path):
    """Verify that cooldown and last request times persist to disk."""
    storage = str(tmp_path)
    api = WebifConnection("10.10.1.225", "user", "pass", storage_path=storage)

    api._cooldown_until = time.monotonic() + 300.0
    api._save_state()

    api_new = WebifConnection("10.10.1.225", "user", "pass", storage_path=storage)

    assert api_new._cooldown_until > time.monotonic() + 298.0
    assert api_new._cooldown_until <= time.monotonic() + 300.0
    assert os.path.exists(os.path.join(storage, "lwp_state.json"))


@pytest.mark.asyncio
async def test_login_redirection(api):
    """Test that the API detects a redirect to login.html as an expired session."""
    async with respx.mock:
        respx.get(url__regex=re.compile(r".*settings_export.html.*")).mock(
            return_value=httpx.Response(303, headers={"Location": "/login.html"})
        )
        respx.get(url__regex=re.compile(r".*login.html.*")).mock(
            return_value=httpx.Response(200, text='<html>name="pass"</html>')
        )
        respx.post(url__regex=re.compile(r".*login.html.*")).mock(
            return_value=httpx.Response(303, headers={"Location": "/home.html"})
        )
        respx.get(url__regex=re.compile(r".*home.html.*")).mock(
            return_value=httpx.Response(200)
        )

        with pytest.raises(McuResourceError):
            await api.update_all(["Heizkreis"])


@pytest.mark.asyncio
async def test_stats_increment(api):
    """Verify that stats increment correctly on success and failure."""
    mock_html = "<html>" + '<div class="col-3"></div>' * 6 + "</html>"

    async with respx.mock:
        respx.get(url__regex=re.compile(r".*settings_export.html.*")).mock(
            return_value=httpx.Response(200, text=mock_html)
        )

        with pytest.raises(McuResourceError):
            await api.update_all(["Statistik"])

        assert api.stats["requests"] == 1
        assert api.stats["successes"] == 0
        assert api.stats["integrity_failures"] == 1
