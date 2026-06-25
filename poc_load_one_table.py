"""Manual proof of concept for the teammate_loader.

Loads one small entity set end to end to verify the loader works before
scaling to all tables. Confirms the page-by-page streaming, the single
transaction load, and the $count reconciliation all behave.
"""
from __future__ import annotations

import asyncio
import logging

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.integrations.teammate_schema_migrate import (
    load_teammate_config,
    build_http_client,
)
# Import the model for the entity you want to test. Adjust the class name
# if your generator named it differently (Terminology vs Terminologies).
from app.db.teammate_models.models_teammate_audit import Terminology

from app.ingestion.teammate_loader import load_entity
from app.ingestion._common import build_db_url, configure_logging

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger("app.ingestion.poc")


def _progress(snapshot: dict) -> None:
    """Print live progress so the POC shows streaming behavior as it runs."""
    logger.info(
        "  progress :: status=%s pages=%s fetched=%s loaded=%s",
        snapshot.get("status"), snapshot.get("pages"),
        snapshot.get("rows_fetched"), snapshot.get("rows_loaded"),
    )


async def main() -> None:
    configure_logging("poc")

    # 1. Load OData config (base_url, headers, etc.)
    cfg = load_teammate_config()

    # 2. Build the async DB session factory.
    engine = create_async_engine(build_db_url(), pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    # 3. Open the OData HTTP client and load one entity set.
    async with build_http_client(cfg) as http_client:
        result = await load_entity(
            schema_name="teammate_audit",
            entity_set="Terminologies",   # OData EntitySet name
            model_class=Terminology,       # SQLAlchemy model class
            http_client=http_client,
            base_url=cfg.base_url,
            session_factory=session_factory,
            mode="full_refresh",
            chunk_size=2000,
            run_id="poc",
            progress_cb=_progress,
        )

    await engine.dispose()

    logger.info("POC result: %s", result)


if __name__ == "__main__":
    asyncio.run(main())
