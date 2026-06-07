# Weishaupt WebIF API

A Python library for interacting asynchronously with the Weishaupt heating system web interface (WebIF). This library allows you to programmatically retrieve telemetry data and system statistics.

## Features

- **Asynchronous Design**: Built on `httpx` and `asyncio` for non-blocking I/O.
- **Selective Updates**: Fetch only the categories you need (e.g., "Statistik", "Heizkreis") to minimize load on the device's MCU.
- **Persistence**: Automatically manages session cookies and internal state (cooldowns, request timing) across restarts using local storage.
- **Smart Batching**: Includes logic to batch requests safely, preventing resource exhaustion on the hardware interface.
- **Robust Error Handling**: Detects session expiration, login redirections, and MCU resource errors.

## Installation

Currently, this library can be installed locally:

```bash
pip install .
```

## Quick Start

```python
import asyncio
from weishaupt_webif_api import WebifConnection

async def main():
    # Initialize the connection
    async with WebifConnection(
        host="10.10.1.225", 
        username="user", 
        password="pass",
        storage_path="./data"
    ) as api:
        # Fetch specific categories
        data = await api.update_all(["Statistik", "Heizkreis"])
        
        for category, values in data.items():
            print(f"--- {category} ---")
            for key, value in values.items():
                print(f"{key}: {value}")

if __name__ == "__main__":
    asyncio.run(main())
```

## Configuration & State

The library creates several files in the `storage_path` to maintain state:

- `lwp_cookies.txt`: Stores session cookies to avoid redundant logins.
- `lwp_state.json`: Stores internal metadata, such as request cooldown timers.
- `weishaupt_webif_api.log`: Contains diagnostic logs for troubleshooting.

These files are excluded from version control by default via `.gitignore`.

## Development

### Running Tests

The project uses `pytest` with `pytest-asyncio` and `respx` for mocking API responses.

```bash
pytest
```

Tests utilize HTML fixtures located in `tests/fixtures/` to simulate real device responses without requiring access to physical hardware.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

*This project is not affiliated with or endorsed by Weishaupt. Use it at your own risk. Frequent polling can put significant stress on the device's web interface.*