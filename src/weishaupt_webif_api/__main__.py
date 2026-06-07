import argparse
import asyncio
import os
import logging
from .api import WebifConnection
from .const import ColoredFormatter


async def _main(args):
    storage_path = args.path or os.getcwd()

    # Configure logging for the console
    logger = logging.getLogger("weishaupt_webif_api")
    logger.setLevel(logging.DEBUG)

    # Console Handler (Colored)
    sh = logging.StreamHandler()
    sh.setFormatter(ColoredFormatter())
    logger.addHandler(sh)

    # File Handler (Persistent log)
    log_file = os.path.join(storage_path, "weishaupt_webif_api.log")
    fh = logging.FileHandler(log_file)
    fh.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )
    logger.addHandler(fh)

    logger.info(f"Starting Weishaupt WebIF API monitor for {args.ip}...")
    async with WebifConnection(
        args.ip, args.user, args.password, storage_path=storage_path
    ) as con:
        try:
            while True:
                try:
                    # Fetch all data
                    data = await con.update_all()
                    summary = ", ".join(
                        [f"[{cat}]: {len(vals)}" for cat, vals in data.items()]
                    )
                    logger.info(f"Update Success: {summary}")
                    await asyncio.sleep(300)
                except (asyncio.CancelledError, KeyboardInterrupt):
                    raise
                except Exception as err:
                    logger.error(f"Update failed: {err}. Retrying in 60 seconds...")
                    await asyncio.sleep(60)
        except (asyncio.CancelledError, KeyboardInterrupt):
            # Graceful exit on Ctrl+C
            pass
        except Exception as err:
            logger.critical(f"Monitor encountered an error: {err}")
        finally:
            logger.info("--- Final Communication Statistics ---")
            for key, value in con.stats.items():
                logger.info(f"{key:20}: {value}")


def main():
    """Sync wrapper for the entry point."""
    parser = argparse.ArgumentParser(description="Weishaupt WebIF API CLI")
    parser.add_argument("ip", help="IP address of the Weishaupt module")
    parser.add_argument("user", help="Login username")
    parser.add_argument("password", help="Login password")
    parser.add_argument(
        "--path",
        help="Directory to store logs and state files (defaults to execution folder)",
        default=None,
    )

    args = parser.parse_args()
    try:
        asyncio.run(_main(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
