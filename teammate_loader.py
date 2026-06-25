from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from datetime import date, datetime
from typing import Any, Callable, Dict, List, Optional, Type
from urllib.parse import urljoin

import httpx
from sqlalchemy import MetaData, insert, text
from sqlalchemy.exc import InterfaceError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

logger = logging.getLogger("app.ingestion.teammate_loader")

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------
HTTP_TIMEOUT_SECONDS: float = 120.0
HTTP_MAX_RETRIES: int = 5
HTTP_RETRY_BACKOFF_SECONDS: float = 2.0
HTTP_RETRY_MAX_BACKOFF_SECONDS: float = 60.0

# Whole-table reconnect retries for transient DB/connection drops. A full
# refresh is idempotent, so re-running the entire table is always safe.
DB_MAX_RETRIES: int = 3
DB_RETRY_BACKOFF_SECONDS: float = 5.0

DEFAULT_PAGE_SIZE: int = 2000

# staging_swap (default): stream into an UNLOGGED staging table (short per-page
#   transactions, no impact on the live table), then swap into the live table in
#   ONE short transaction via DELETE + INSERT...SELECT. Readers are never blocked
#   (ROW EXCLUSIVE does not conflict with SELECT's ACCESS SHARE) and see either
#   the full old snapshot or the full new one — never a partial load. Preserves
#   the live table's identity, FKs, indexes, and grants.
# single_txn: TRUNCATE + all inserts in ONE transaction. Atomic, but holds an
#   ACCESS EXCLUSIVE lock for the whole load, so readers are blocked. Kept as an
#   opt-in for environments that want strict single-transaction semantics.
LOAD_STRATEGY: str = os.environ.get("INGEST_LOAD_STRATEGY", "staging_swap")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _strip_odata_keys(row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in row.items() if not k.startswith("@odata.")}


def _coerce_value(value: Any, column_type: Any) -> Any:
    if value is None:
        return None

    type_name = type(column_type).__name__

    if type_name == "Date":
        if isinstance(value, str):
            return date.fromisoformat(value[:10])
        return value

    if type_name == "DateTime":
        if isinstance(value, str):
            cleaned = value.replace("Z", "+00:00")
            return datetime.fromisoformat(cleaned)
        return value

    return value


def _coerce_row(row: Dict[str, Any], model_class: Type) -> Dict[str, Any]:
    stripped = _strip_odata_keys(row)
    coerced: Dict[str, Any] = {}
    for column in model_class.__table__.columns:
        col_name = column.name
        if col_name in stripped:
            coerced[col_name] = _coerce_value(stripped[col_name], column.type)
    return coerced


def _safe_progress(progress_cb: Optional[Callable[[Dict[str, Any]], None]],
                   snapshot: Dict[str, Any]) -> None:
    """Invoke the progress callback without ever letting it break the load."""
    if progress_cb is None:
        return
    try:
        progress_cb(dict(snapshot))
    except Exception:  # pragma: no cover - progress is best-effort
        logger.debug("progress callback raised", exc_info=True)


# ---------------------------------------------------------------------------
# OData fetch with timeout and retry, one page at a time
# ---------------------------------------------------------------------------
def _retry_after_seconds(response: httpx.Response) -> Optional[float]:
    """Parse a numeric Retry-After header; ignore HTTP-date form."""
    raw = response.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


async def _get_with_retry(client: Any, url: str) -> Dict[str, Any]:
    """GET one URL with timeout, retrying transient errors.

    Transient = network/timeout, HTTP 5xx, or HTTP 429 (rate limit). Backoff is
    exponential with jitter and honors Retry-After when the server sends it.
    """
    last_exc: Exception | None = None

    for attempt in range(1, HTTP_MAX_RETRIES + 1):
        try:
            response = await client.get(url, timeout=HTTP_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.json()
        except (httpx.TimeoutException, httpx.HTTPStatusError, httpx.RequestError) as exc:
            last_exc = exc
            status = (
                exc.response.status_code
                if isinstance(exc, httpx.HTTPStatusError) else None
            )
            transient = (
                isinstance(exc, (httpx.TimeoutException, httpx.RequestError))
                or (status is not None and (status == 429 or status >= 500))
            )
            if not transient or attempt == HTTP_MAX_RETRIES:
                raise

            retry_after = (
                _retry_after_seconds(exc.response)
                if isinstance(exc, httpx.HTTPStatusError) else None
            )
            if retry_after is not None:
                backoff = retry_after
            else:
                backoff = min(
                    HTTP_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1)),
                    HTTP_RETRY_MAX_BACKOFF_SECONDS,
                )
                backoff += random.uniform(0, backoff * 0.25)  # jitter

            logger.warning(
                "HTTP error (%s) on attempt %d for %s: %s. Retrying in %.1fs.",
                status or "network", attempt, url, exc, backoff,
            )
            await asyncio.sleep(backoff)

    # Should not reach here, but keep type checkers happy.
    assert last_exc is not None
    raise last_exc


async def _fetch_count(client: Any, base_url: str, entity_set: str) -> Optional[int]:
    """Best-effort source row count via OData ``/$count`` for reconciliation.

    Returns None (and logs) for entity sets that do not support $count, so the
    reconciliation step is simply skipped rather than failing the load.
    """
    url = f"{base_url}/{entity_set}/$count"
    try:
        response = await client.get(url, timeout=HTTP_TIMEOUT_SECONDS)
        response.raise_for_status()
        return int(response.text.strip())
    except Exception as exc:  # noqa: BLE001 - reconciliation is best-effort
        logger.info(
            "[%s] $count unavailable (%s); skipping reconciliation.",
            entity_set, type(exc).__name__,
        )
        return None


def _normalize_next_link(next_link: str, base_url: str) -> str:
    """OData @odata.nextLink may be absolute or relative; normalize to absolute."""
    if next_link.startswith(("http://", "https://")):
        return next_link
    return urljoin(base_url.rstrip("/") + "/", next_link.lstrip("/"))


# ---------------------------------------------------------------------------
# DB write helpers
# ---------------------------------------------------------------------------
async def _truncate_table(session: AsyncSession, schema: str, table: str) -> None:
    stmt = text(f'TRUNCATE TABLE "{schema}"."{table}"')
    await session.execute(stmt)


async def _insert_rows(
        session: AsyncSession,
        model_class: Type,
        rows: List[Dict[str, Any]],
) -> int:
    if not rows:
        return 0
    await session.execute(insert(model_class), rows)
    return len(rows)


# ---------------------------------------------------------------------------
# Load strategies
# ---------------------------------------------------------------------------
async def _run_single_txn_load(
        session_factory: async_sessionmaker[AsyncSession],
        model_class: Type,
        schema_name: str,
        entity_set: str,
        http_client: Any,
        base_url: str,
        page_size: int,
        result: Dict[str, Any],
        progress_cb: Optional[Callable[[Dict[str, Any]], None]],
) -> tuple[int, int, int]:
    """TRUNCATE + stream every page into Postgres inside ONE transaction.

    Memory stays bounded (each page is inserted then discarded), while atomicity
    guarantees the table is either fully replaced or left exactly as it was.
    Pagination follows the server-driven @odata.nextLink chain, which avoids the
    skipped/duplicated rows that deep $skip/$top offsets cause.
    """
    rows_fetched = 0
    rows_loaded = 0
    page_num = 0
    url: Optional[str] = f"{base_url}/{entity_set}?$top={page_size}"

    async with session_factory() as session:
        async with session.begin():
            await _truncate_table(session, schema_name, model_class.__tablename__)

            while url:
                page_num += 1
                payload = await _get_with_retry(http_client, url)
                page_rows = payload.get("value") or []
                rows_fetched += len(page_rows)

                if page_rows:
                    coerced = [_coerce_row(r, model_class) for r in page_rows]
                    rows_loaded += await _insert_rows(session, model_class, coerced)

                next_link = payload.get("@odata.nextLink")
                url = _normalize_next_link(next_link, base_url) if next_link else None

                if page_num % 10 == 0:
                    logger.info(
                        "[%s.%s] page %d, %d rows so far",
                        schema_name, entity_set, page_num, rows_loaded,
                    )
                _safe_progress(progress_cb, {
                    **result,
                    "rows_fetched": rows_fetched,
                    "rows_loaded": rows_loaded,
                    "pages": page_num,
                    "status": "running",
                })
        # Commit happens on clean exit of session.begin(); any exception above
        # rolls the TRUNCATE back too, restoring the previous data.

    return rows_fetched, rows_loaded, page_num


def _staging_relation(model_class: Type, schema_name: str) -> Any:
    """A SQLAlchemy Core Table aimed at the ``<table>__staging`` relation.

    Cloned from the model so coerced rows insert with the exact same columns,
    without declaring a second ORM model.
    """
    staging_name = f"{model_class.__tablename__}__staging"
    return model_class.__table__.to_metadata(
        MetaData(), schema=schema_name, name=staging_name,
    )


async def _run_staging_swap_load(
        session_factory: async_sessionmaker[AsyncSession],
        model_class: Type,
        schema_name: str,
        entity_set: str,
        http_client: Any,
        base_url: str,
        page_size: int,
        result: Dict[str, Any],
        progress_cb: Optional[Callable[[Dict[str, Any]], None]],
) -> tuple[int, int, int]:
    """Non-blocking load: fill a staging table, then atomically swap into live.

    Readers of the live table are never blocked and never see a partial load.
    The slow OData fetch happens entirely against the UNLOGGED staging table in
    short per-page transactions; only the final DELETE + INSERT...SELECT touches
    the live table, in one short transaction that MVCC keeps invisible to
    concurrent SELECTs until it commits.
    """
    table = model_class.__tablename__
    staging = f"{table}__staging"
    qlive = f'"{schema_name}"."{table}"'
    qstaging = f'"{schema_name}"."{staging}"'
    columns = [c.name for c in model_class.__table__.columns]
    col_list = ", ".join(f'"{c}"' for c in columns)
    staging_table = _staging_relation(model_class, schema_name)

    # 1. Fresh, empty staging table that mirrors the current live definition.
    #    UNLOGGED keeps the staging load fast and out of the WAL; DROP+CREATE
    #    guarantees the schema matches even after a prior aborted run.
    async with session_factory() as session:
        async with session.begin():
            await session.execute(text(f"DROP TABLE IF EXISTS {qstaging}"))
            await session.execute(text(
                f"CREATE UNLOGGED TABLE {qstaging} "
                f"(LIKE {qlive} INCLUDING DEFAULTS)"
            ))

    # 2. Stream every @odata.nextLink page into staging (short transactions).
    rows_fetched = 0
    rows_loaded = 0
    page_num = 0
    url: Optional[str] = f"{base_url}/{entity_set}?$top={page_size}"

    while url:
        page_num += 1
        payload = await _get_with_retry(http_client, url)
        page_rows = payload.get("value") or []
        rows_fetched += len(page_rows)

        if page_rows:
            coerced = [_coerce_row(r, model_class) for r in page_rows]
            async with session_factory() as session:
                async with session.begin():
                    await session.execute(insert(staging_table), coerced)
            rows_loaded += len(coerced)

        next_link = payload.get("@odata.nextLink")
        url = _normalize_next_link(next_link, base_url) if next_link else None

        if page_num % 10 == 0:
            logger.info(
                "[%s.%s] (staging) page %d, %d rows so far",
                schema_name, entity_set, page_num, rows_loaded,
            )
        _safe_progress(progress_cb, {
            **result,
            "rows_fetched": rows_fetched,
            "rows_loaded": rows_loaded,
            "pages": page_num,
            "status": "running",
        })

    # 3. Atomic, reader-safe swap. DELETE + INSERT...SELECT take ROW EXCLUSIVE,
    #    which does not conflict with readers' ACCESS SHARE, and the whole swap
    #    commits as one unit.
    async with session_factory() as session:
        async with session.begin():
            await session.execute(text(f"DELETE FROM {qlive}"))
            await session.execute(text(
                f"INSERT INTO {qlive} ({col_list}) "
                f"SELECT {col_list} FROM {qstaging}"
            ))
    logger.info(
        "[%s.%s] swapped %d rows into live table (non-blocking)",
        schema_name, entity_set, rows_loaded,
    )

    # 4. Drop staging and refresh planner stats (best-effort; never fails the run).
    try:
        async with session_factory() as session:
            async with session.begin():
                await session.execute(text(f"DROP TABLE IF EXISTS {qstaging}"))
                await session.execute(text(f"ANALYZE {qlive}"))
    except Exception:  # noqa: BLE001 - cleanup/stats are non-critical
        logger.warning(
            "[%s.%s] staging cleanup/ANALYZE failed (non-fatal)",
            schema_name, entity_set, exc_info=True,
        )

    return rows_fetched, rows_loaded, page_num


# ---------------------------------------------------------------------------
# Public API, streaming variant
# ---------------------------------------------------------------------------
async def load_entity(
        schema_name: str,
        entity_set: str,
        model_class: Type,
        http_client: Any,
        base_url: str,
        session_factory: async_sessionmaker[AsyncSession],
        mode: str = "full_refresh",
        chunk_size: int = 2000,
        run_id: Optional[str] = None,
        progress_cb: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    """Load one OData entity set into Postgres, streaming page by page.

    Behavior:
      1. Best-effort fetch the source ``$count`` for later reconciliation.
      2. Run the configured load strategy (staging_swap by default): stream every
         @odata.nextLink page into an UNLOGGED staging table, then atomically
         swap it into the live table without blocking readers.
      3. On a transient DB disconnect, reconnect and retry the whole (idempotent)
         table up to DB_MAX_RETRIES times.
      4. Reconcile rows_loaded against the source $count; a mismatch fails the load.

    The signature and the original result keys are preserved; ``run_id``,
    ``rows_expected``, and ``attempts`` are additive. ``progress_cb`` (additive,
    optional) receives a live snapshot per page and at completion.
    """
    start_time = time.monotonic()
    result: Dict[str, Any] = {
        "schema": schema_name,
        "entity_set": entity_set,
        "table": model_class.__tablename__,
        "mode": mode,
        "run_id": run_id,
        "rows_fetched": 0,
        "rows_loaded": 0,
        "rows_expected": None,
        "pages": 0,
        "attempts": 0,
        "duration_seconds": 0.0,
        "status": "running",
        "error": None,
    }

    try:
        if mode != "full_refresh":
            raise NotImplementedError(
                f"mode={mode} is not yet implemented. Pass 1 supports full_refresh only."
            )
        if LOAD_STRATEGY not in ("single_txn", "staging_swap"):
            raise ValueError(
                f"Unknown INGEST_LOAD_STRATEGY={LOAD_STRATEGY!r} "
                f"(expected 'single_txn' or 'staging_swap')."
            )

        page_size = chunk_size or DEFAULT_PAGE_SIZE

        # Reconciliation target, fetched once up front.
        result["rows_expected"] = await _fetch_count(http_client, base_url, entity_set)

        logger.info(
            "[%s.%s] starting load (strategy=%s, expected=%s)",
            schema_name, entity_set, LOAD_STRATEGY, result["rows_expected"],
        )

        runner = (
            _run_single_txn_load
            if LOAD_STRATEGY == "single_txn"
            else _run_staging_swap_load
        )

        last_db_exc: Exception | None = None
        for attempt in range(1, DB_MAX_RETRIES + 1):
            result["attempts"] = attempt
            try:
                rows_fetched, rows_loaded, pages = await runner(
                    session_factory, model_class, schema_name, entity_set,
                    http_client, base_url, page_size, result, progress_cb,
                )
                result["rows_fetched"] = rows_fetched
                result["rows_loaded"] = rows_loaded
                result["pages"] = pages
                break
            except (OperationalError, InterfaceError) as exc:
                # Transient DB/connection drop (Azure Postgres closes idle/long
                # connections). The whole-table retry is the "pause & reconnect".
                last_db_exc = exc
                if attempt == DB_MAX_RETRIES:
                    raise
                backoff = DB_RETRY_BACKOFF_SECONDS * attempt
                logger.warning(
                    "[%s.%s] DB error on attempt %d: %s. Reconnecting in %.1fs.",
                    schema_name, entity_set, attempt, exc, backoff,
                )
                await asyncio.sleep(backoff)

        # Reconciliation: a silent partial load must not pass as success.
        expected = result["rows_expected"]
        if expected is not None and result["rows_loaded"] != expected:
            raise RuntimeError(
                f"row count mismatch: loaded {result['rows_loaded']} "
                f"but source $count is {expected}"
            )

        result["status"] = "success"

    except Exception as exc:
        logger.exception("Failed to load %s.%s", schema_name, entity_set)
        result["status"] = "failed"
        result["error"] = f"{type(exc).__name__}: {exc}"

    result["duration_seconds"] = round(time.monotonic() - start_time, 2)

    if result["status"] == "success":
        logger.info(
            "[%s.%s] %d pages, %d rows fetched, %d rows loaded in %.2fs",
            schema_name, entity_set,
            result["pages"], result["rows_fetched"], result["rows_loaded"],
            result["duration_seconds"],
        )
    else:
        logger.error(
            "[%s.%s] FAILED in %.2fs: %s",
            schema_name, entity_set,
            result["duration_seconds"], result["error"],
        )

    _safe_progress(progress_cb, result)
    return result
