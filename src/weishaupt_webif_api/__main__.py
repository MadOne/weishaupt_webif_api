"""Command line interface for the Weishaupt WebIF API."""

import argparse
import asyncio
import contextlib
import logging
import os
from pathlib import Path

from .api import WebifConnection
from .const import ColoredFormatter


async def _main(args: argparse.Namespace) -> None:
    storage_path = Path(args.path) if args.path else Path.cwd()

    # Configure logging for the console
    logger = logging.getLogger("weishaupt_webif_api")
    log_level = logging.DEBUG if args.debug else logging.INFO
    logger.setLevel(log_level)

    # Console Handler (Colored)
    sh = logging.StreamHandler()
    sh.setFormatter(ColoredFormatter())
    logger.addHandler(sh)

    # File Handler (Persistent log)
    log_file = storage_path / "weishaupt_webif_api.log"
    fh = logging.FileHandler(str(log_file))
    fh.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"),
    )
    logger.addHandler(fh)

    logger.info("Starting Weishaupt WebIF API monitor for %s...", args.ip)
    async with WebifConnection(
        args.ip,
        args.user,
        args.password,
        request_delay=args.request_delay,
        cooldown_delay=args.cooldown_delay,
        storage_path=storage_path,
    ) as con:
        try:
            while True:
                try:
                    # Fetch all data
                    data = await con.update_all()
                    summary = ", ".join(
                        [f"[{cat}]: {len(vals)}" for cat, vals in data.items()],
                    )
                    logger.info(
                        "Update Success: %s. Next check in %ds.",
                        summary,
                        args.interval,
                    )
                    await asyncio.sleep(args.interval)
                except (asyncio.CancelledError, KeyboardInterrupt):
                    raise
                except Exception:
                    logger.exception(
                        "Update failed. Retrying in %ds...",
                        args.retry_interval,
                    )
                    await asyncio.sleep(args.retry_interval)
        except (asyncio.CancelledError, KeyboardInterrupt):
            # Graceful exit on Ctrl+C
            pass
        except Exception as err:  # noqa: BLE001
            logger.critical("Monitor encountered an error: %s", err)
        finally:
            logger.info("--- Final Communication Statistics ---")
            for key, value in con.stats.items():
                logger.info("%-20s: %s", key, value)


def main() -> None:
    """Sync wrapper for the entry point."""
    parser = argparse.ArgumentParser(
        description="Weishaupt WebIF API CLI",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "ip",
        nargs="?" if os.environ.get("WEISHAUPT_IP") else None,
        default=os.environ.get("WEISHAUPT_IP"),
        help="IP address of the Weishaupt module (env: WEISHAUPT_IP)",
    )
    parser.add_argument(
        "user",
        nargs="?" if os.environ.get("WEISHAUPT_USER") else None,
        default=os.environ.get("WEISHAUPT_USER"),
        help="Login username (env: WEISHAUPT_USER)",
    )
    parser.add_argument(
        "password",
        nargs="?" if os.environ.get("WEISHAUPT_PASSWORD") else None,
        default=os.environ.get("WEISHAUPT_PASSWORD"),
        help="Login password (env: WEISHAUPT_PASSWORD)",
    )
    parser.add_argument(
        "--path",
        help="Directory to store logs and state files (defaults to execution folder)",
        default=None,
    )
    parser.add_argument(
        "--interval",
        type=int,
        help="Polling interval in seconds (default: 300)",
        default=300,
    )
    parser.add_argument(
        "--retry-interval",
        type=int,
        help="Retry interval after failure in seconds",
        default=60,
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        help="Internal breather delay between requests in seconds",
        default=60.0,
    )
    parser.add_argument(
        "--cooldown-delay",
        type=float,
        help="Internal cooldown penalty after failure in seconds",
        default=300.0,
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    if not all([args.ip, args.user, args.password]):
        parser.error(
            "IP, user, and password must be provided via arguments or "
            "environment variables.",
        )

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(_main(args))


if __name__ == "__main__":
    main()
