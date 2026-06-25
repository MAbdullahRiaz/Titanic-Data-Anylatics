"""Diagnostic: validate server-driven @odata.nextLink pagination.

The loader follows the OData @odata.nextLink chain rather than guessing deep
$skip/$top offsets (which can silently skip or duplicate rows). This script
walks the nextLink chain for an entity set, confirms each page advances, and
checks the running total against the source $count so you can prove the chain
is continuous before trusting a full load.

Usage:
    python -m app.ingestion.diag_check_pagination [EntitySet] [--max-pages N]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from urllib.parse import urljoin

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from app.integrations.teammate_schema_migrate import (
    load_teammate_config,
    build_http_client,
)
from app.ingestion._common import configure_logging

logger = logging.getLogger("diag")

DEFAULT_ENTITY = "DimensionAssignmentEntity"
PAGE_SIZE = 2000


def _normalize_next_link(next_link: str, base_url: str) -> str:
    if next_link.startswith(("http://", "https://")):
        return next_link
    return urljoin(base_url.rstrip("/") + "/", next_link.lstrip("/"))


async def _fetch_count(client, base_url: str, entity_set: str) -> int | None:
    try:
        response = await client.get(f"{base_url}/{entity_set}/$count", timeout=60.0)
        response.raise_for_status()
        return int(response.text.strip())
    except Exception as exc:  # noqa: BLE001
        logger.info("$count unavailable (%s)", type(exc).__name__)
        return None


async def main() -> None:
    configure_logging("diag")
    parser = argparse.ArgumentParser(description="Validate @odata.nextLink pagination.")
    parser.add_argument("entity", nargs="?", default=DEFAULT_ENTITY, help="EntitySet name.")
    parser.add_argument("--max-pages", type=int, default=5, help="Pages to walk before stopping.")
    args = parser.parse_args()

    cfg = load_teammate_config()

    async with build_http_client(cfg) as client:
        expected = await _fetch_count(client, cfg.base_url, args.entity)
        logger.info("Entity: %s  source $count: %s", args.entity, expected)

        url = f"{cfg.base_url}/{args.entity}?$top={PAGE_SIZE}"
        page = 0
        running_total = 0
        seen_links: set[str] = set()

        while url and page < args.max_pages:
            page += 1
            logger.info("Probing page %d: %s", page, url)
            response = await client.get(url, timeout=60.0)
            response.raise_for_status()
            payload = response.json()

            rows = payload.get("value") or []
            running_total += len(rows)
            next_link = payload.get("@odata.nextLink")

            logger.info("  status: %s", response.status_code)
            logger.info("  rows in value: %d (running total %d)", len(rows), running_total)
            logger.info("  has @odata.nextLink: %s", bool(next_link))

            if next_link:
                normalized = _normalize_next_link(next_link, cfg.base_url)
                if normalized in seen_links:
                    logger.error("  LOOP DETECTED: nextLink repeats -> %s", normalized[:200])
                    break
                seen_links.add(normalized)
                logger.info("  nextLink: %s", normalized[:200])
                url = normalized
            else:
                logger.info("  end of chain after %d pages, %d rows", page, running_total)
                url = None

        if expected is not None and url is None:
            status = "MATCH" if running_total == expected else "MISMATCH"
            logger.info("Walked full chain: %d rows vs $count %d -> %s",
                        running_total, expected, status)


if __name__ == "__main__":
    asyncio.run(main())
