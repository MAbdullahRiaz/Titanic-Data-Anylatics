"""Shared helpers for the ingestion package.

Centralizes the Postgres URL builder, filesystem layout, and logging setup so
the loader, driver, POC, diagnostic, and scheduled entrypoint stay consistent
and Azure-friendly (no reliance on the current working directory).
"""
from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

# Resolve all default paths relative to THIS file, never the process CWD.
# Azure App Service / WebJobs do not guarantee the CWD, so CWD-relative paths
# (the old ``Path("./../sample_data")``) break silently in the dev slot.
_THIS_DIR: Path = Path(__file__).resolve().parent
_REPO_ROOT: Path = _THIS_DIR.parent


def build_db_url() -> str:
    """Build the asyncpg Postgres URL from the environment.

    SSL is required (Azure Database for PostgreSQL enforces it). Keeping the
    builder in one place avoids the three divergent copies that existed before.
    """
    host = os.environ["DB_HOST"]
    port = os.environ.get("DB_PORT", "5432")
    user = os.environ["DB_USER"]
    password = os.environ["DB_PASSWORD"]
    name = os.environ["DB_NAME"]
    return f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{name}?ssl=require"


def data_root() -> Path:
    """Root of the generated sample_data manifests (override: INGEST_DATA_ROOT)."""
    return Path(os.environ.get("INGEST_DATA_ROOT", str(_REPO_ROOT / "sample_data")))


def log_dir() -> Path:
    """Directory for persisted run logs (override: INGEST_LOG_DIR)."""
    path = Path(os.environ.get("INGEST_LOG_DIR", str(_THIS_DIR / "logs")))
    path.mkdir(parents=True, exist_ok=True)
    return path


def state_dir() -> Path:
    """Directory for run-status JSON and the failures file (override: INGEST_STATE_DIR)."""
    path = Path(os.environ.get("INGEST_STATE_DIR", str(_THIS_DIR / "state")))
    path.mkdir(parents=True, exist_ok=True)
    return path


_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s :: %(message)s"


def configure_logging(run_id: str) -> Path:
    """Send logs to both the console and a rotating per-run file.

    Returns the path of the log file so the caller can surface it. Attaching to
    the root logger means every ``app.*`` logger is captured, including the
    loader's ``logger.exception`` failure traces — exactly the persisted
    "why it broke" record the pipeline needs.
    """
    log_path = log_dir() / f"ingest_{run_id}.log"
    formatter = logging.Formatter(_LOG_FORMAT)

    file_handler = RotatingFileHandler(
        log_path, maxBytes=20_000_000, backupCount=5, encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # Avoid duplicate handlers if configure_logging is called more than once.
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(file_handler)
    root.addHandler(console_handler)

    return log_path
