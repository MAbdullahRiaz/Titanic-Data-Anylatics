"""Standalone scheduled entrypoint for the Azure dev slot.

Deployed alongside the web app and triggered on a CRON schedule (see
``settings.job`` for an Azure triggered-WebJob schedule). Wraps ``run_backfill``
with the two things a scheduled, unattended job needs that a manual CLI does not:

  * Single-run guard: a Postgres advisory lock so an overlapping trigger or a
    slot swap cannot start a second concurrent migration. If the lock is held,
    this run exits cleanly without doing anything.
  * Graceful shutdown: on SIGTERM (Azure stops the slot during a swap/restart),
    finish the current table, flush logs/status, then stop — no half-written
    table is left behind because each table load is itself atomic.

Exit code is non-zero if any table failed, so Azure marks the run failed.
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.ingestion.run_full_backfill import run_backfill
from app.ingestion._common import build_db_url, configure_logging

logger = logging.getLogger("app.ingestion.run_scheduled")

# Arbitrary but fixed application-wide key for the migration advisory lock.
ADVISORY_LOCK_KEY = 8_273_001


def _install_shutdown_handler(stop_flag: dict) -> None:
    """Flip stop_flag on SIGTERM/SIGINT so run_backfill stops after this table."""

    def _handle(*_args) -> None:
        if not stop_flag["stop"]:
            logger.warning("Shutdown signal received; stopping after current table.")
        stop_flag["stop"] = True

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle)
        except (NotImplementedError, RuntimeError):
            # Windows event loop does not support add_signal_handler for SIGTERM.
            signal.signal(sig, lambda *_: _handle())


async def main() -> int:
    configure_logging("scheduler_bootstrap")
    stop_flag = {"stop": False}
    _install_shutdown_handler(stop_flag)

    engine = create_async_engine(build_db_url(), pool_pre_ping=True)
    try:
        # Hold the advisory lock on a single dedicated connection for the run.
        async with engine.connect() as conn:
            acquired = await conn.scalar(
                text("SELECT pg_try_advisory_lock(:k)"), {"k": ADVISORY_LOCK_KEY}
            )
            if not acquired:
                logger.warning(
                    "Advisory lock %s is held by another run; exiting without action.",
                    ADVISORY_LOCK_KEY,
                )
                return 0

            logger.info("Advisory lock acquired; starting scheduled backfill.")
            try:
                summary = await run_backfill(should_stop=lambda: stop_flag["stop"])
            finally:
                await conn.scalar(
                    text("SELECT pg_advisory_unlock(:k)"), {"k": ADVISORY_LOCK_KEY}
                )
                logger.info("Advisory lock released.")
    finally:
        await engine.dispose()

    if summary.get("stopped_early"):
        logger.warning("Run %s stopped early; treating as failure.", summary.get("run_id"))
        return 1
    return 1 if summary.get("tables_failed", 0) else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
