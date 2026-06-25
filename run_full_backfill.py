"""Full backfill driver for all TeamMate OData entity sets.

Reads the manifest produced by step 5 to know which entity sets exist per
schema, then calls the loader for each one. A single table failure does not
block the rest of the run. Progress and failures are persisted to disk while
the run is in flight (rotating log file + a live run-status JSON), and a
failures file is written at the end for easy rerun.
"""
from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import logging
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.integrations.teammate_schema_migrate import (
    load_teammate_config,
    build_http_client,
)
from app.ingestion.teammate_loader import load_entity
from app.ingestion._common import (
    build_db_url,
    configure_logging,
    data_root,
    state_dir,
)

logger = logging.getLogger("app.ingestion.run_full_backfill")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MANIFEST_FILENAME: str = "_manifest.json"
FAILURES_FILENAME: str = "backfill_failures.json"

SCHEMAS: List[Tuple[str, str]] = [
    # (schema_name, model_module_path)
    ("teammate_audit", "app.db.teammate_models.models_teammate_audit"),
    ("teammate_controls", "app.db.teammate_models.models_teammate_controls"),
]


# ---------------------------------------------------------------------------
# Live run-status writer
# ---------------------------------------------------------------------------
class StatusWriter:
    """Persist a single run-status JSON that is updated as the run progresses.

    Writes atomically (temp file + replace) so a reader tailing the file during
    a run never sees a half-written document. This is what makes live progress
    and the "why it broke" reason observable while the pipeline is still running.
    """

    def __init__(self, path: Path, run_id: str) -> None:
        self.path = path
        self.run_id = run_id
        self.started_at = datetime.now(timezone.utc).isoformat()
        self._entities: Dict[str, Dict[str, Any]] = {}

    def update(self, snapshot: Dict[str, Any]) -> None:
        key = f"{snapshot.get('schema')}.{snapshot.get('entity_set')}"
        self._entities[key] = snapshot
        self._flush(status="running")

    def finalize(self, results: List[Dict[str, Any]], total_seconds: float) -> None:
        failed = [r for r in results if r.get("status") != "success"]
        self._flush(
            status="completed",
            total_seconds=total_seconds,
            tables_total=len(results),
            tables_failed=len(failed),
        )

    def _flush(self, status: str, **extra: Any) -> None:
        doc = {
            "run_id": self.run_id,
            "status": status,
            "started_at": self.started_at,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "entities": list(self._entities.values()),
            **extra,
        }
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(doc, indent=2, default=str), encoding="utf-8")
        tmp.replace(self.path)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _load_manifest(schema_name: str) -> Dict[str, Any]:
    """Load the manifest JSON written by step 5 for one schema."""
    path = data_root() / schema_name / MANIFEST_FILENAME
    if not path.exists():
        raise FileNotFoundError(f"Manifest not found for {schema_name} at {path}")
    return json.loads(path.read_text(encoding="utf8"))


def _resolve_model_class(module: Any, table_name: str) -> Any:
    """Find the SQLAlchemy model class whose __tablename__ matches.

    The generated module declares many model classes. We pick the one whose
    declared __tablename__ matches the EntitySet name from the manifest.
    """
    for attr_name in dir(module):
        obj = getattr(module, attr_name)
        tablename = getattr(obj, "__tablename__", None)
        if tablename == table_name:
            return obj
    return None


def _filter_entity_sets(
        entity_sets: Dict[str, str],
        only: List[str] | None,
        skip: List[str] | None,
) -> Dict[str, str]:
    if only:
        return {k: v for k, v in entity_sets.items() if k in only}
    if skip:
        return {k: v for k, v in entity_sets.items() if k not in skip}
    return entity_sets


# ---------------------------------------------------------------------------
# Main driver
# ---------------------------------------------------------------------------
async def run_backfill(
        only_failed: bool = False,
        only_entities: List[str] | None = None,
        should_stop: Optional[Callable[[], bool]] = None,
) -> Dict[str, Any]:
    """Run the backfill. Returns a small summary dict.

    ``should_stop`` (additive, optional) is polled between tables so a scheduled
    runner can stop gracefully on SIGTERM after the current table finishes.
    """
    overall_start = time.monotonic()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]

    log_path = configure_logging(run_id)
    status_writer = StatusWriter(state_dir() / f"run_{run_id}.json", run_id)
    logger.info("Run %s started. Logging to %s", run_id, log_path)

    cfg = load_teammate_config()
    engine = create_async_engine(build_db_url(), pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    all_results: List[Dict[str, Any]] = []
    previous_failures: List[str] = []
    stopped_early = False

    if only_failed:
        failures_path = state_dir() / FAILURES_FILENAME
        if not failures_path.exists():
            logger.error("No previous failures file at %s. Nothing to rerun.", failures_path)
            await engine.dispose()
            return {"run_id": run_id, "tables_total": 0, "tables_failed": 0}
        previous_failures = json.loads(failures_path.read_text(encoding="utf8"))
        logger.info("Rerunning %d previously failed entity sets only.", len(previous_failures))

    async with build_http_client(cfg) as http_client:
        for schema_name, module_path in SCHEMAS:
            if should_stop is not None and should_stop():
                stopped_early = True
                break

            logger.info("=" * 70)
            logger.info("Schema: %s", schema_name)
            logger.info("=" * 70)

            manifest = _load_manifest(schema_name)
            entity_sets: Dict[str, str] = manifest.get("entity_sets") or {}

            if only_failed:
                schema_failures = [
                    name for name in entity_sets
                    if f"{schema_name}.{name}" in previous_failures
                ]
                entity_sets = {k: entity_sets[k] for k in schema_failures}

            if only_entities:
                entity_sets = _filter_entity_sets(entity_sets, only_entities, None)

            if not entity_sets:
                logger.info("No entity sets to load for %s. Skipping.", schema_name)
                continue

            module = importlib.import_module(module_path)

            for entity_set in sorted(entity_sets):
                if should_stop is not None and should_stop():
                    stopped_early = True
                    break

                model_class = _resolve_model_class(module, entity_set)
                if model_class is None:
                    logger.error(
                        "No model class found for %s.%s. Skipping.",
                        schema_name, entity_set,
                    )
                    miss = {
                        "schema": schema_name,
                        "entity_set": entity_set,
                        "table": entity_set,
                        "mode": "full_refresh",
                        "run_id": run_id,
                        "rows_fetched": 0,
                        "rows_loaded": 0,
                        "duration_seconds": 0.0,
                        "status": "failed",
                        "error": "model_class_not_found",
                    }
                    all_results.append(miss)
                    status_writer.update(miss)
                    continue

                result = await load_entity(
                    schema_name=schema_name,
                    entity_set=entity_set,
                    model_class=model_class,
                    http_client=http_client,
                    base_url=cfg.base_url,
                    session_factory=session_factory,
                    mode="full_refresh",
                    chunk_size=2000,
                    run_id=run_id,
                    progress_cb=status_writer.update,
                )
                all_results.append(result)
                status_writer.update(result)

            if stopped_early:
                break

    await engine.dispose()

    if stopped_early:
        logger.warning("Run %s stopped early on shutdown request.", run_id)

    total_seconds = round(time.monotonic() - overall_start, 2)
    _print_summary(all_results, total_seconds)
    _write_failures_file(all_results)
    status_writer.finalize(all_results, total_seconds)

    failed = [r for r in all_results if r["status"] != "success"]
    return {
        "run_id": run_id,
        "tables_total": len(all_results),
        "tables_failed": len(failed),
        "stopped_early": stopped_early,
        "log_path": str(log_path),
    }


def _print_summary(results: List[Dict[str, Any]], total_seconds: float) -> None:
    success = [r for r in results if r["status"] == "success"]
    failed = [r for r in results if r["status"] != "success"]
    total_rows = sum(r["rows_loaded"] for r in success)

    logger.info("")
    logger.info("=" * 70)
    logger.info("BACKFILL SUMMARY")
    logger.info("=" * 70)
    logger.info("Total tables attempted: %d", len(results))
    logger.info("Successful:             %d", len(success))
    logger.info("Failed:                 %d", len(failed))
    logger.info("Total rows loaded:      %d", total_rows)
    logger.info("Total wall clock time:  %.2fs (%.2f min)", total_seconds, total_seconds / 60)

    if failed:
        logger.info("")
        logger.info("Failed tables:")
        for r in failed:
            logger.info("  %s.%s :: %s", r["schema"], r["entity_set"], r["error"])

    # Slowest 10 for quick visibility into what to optimize later.
    slowest = sorted(success, key=lambda r: r["duration_seconds"], reverse=True)[:10]
    if slowest:
        logger.info("")
        logger.info("Slowest 10 successful tables:")
        for r in slowest:
            logger.info(
                "  %s.%s :: %d rows in %.2fs",
                r["schema"], r["entity_set"], r["rows_loaded"], r["duration_seconds"],
            )


def _write_failures_file(results: List[Dict[str, Any]]) -> None:
    failures = [
        f"{r['schema']}.{r['entity_set']}"
        for r in results
        if r["status"] != "success"
    ]
    path = state_dir() / FAILURES_FILENAME
    path.write_text(json.dumps(failures, indent=2), encoding="utf8")
    logger.info("")
    logger.info("Failures written to: %s", path.resolve())


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TeamMate full backfill.")
    parser.add_argument(
        "--only-failed",
        action="store_true",
        help="Only rerun entity sets listed in backfill_failures.json from a previous run.",
    )
    parser.add_argument(
        "--only",
        nargs="+",
        default=None,
        help="Run only these specific entity sets (e.g. --only Terminologies Issues).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    summary = asyncio.run(run_backfill(
        only_failed=args.only_failed,
        only_entities=args.only,
    ))
    # Non-zero exit on any failure so schedulers/CI mark the run as failed.
    sys.exit(1 if summary.get("tables_failed", 0) else 0)
