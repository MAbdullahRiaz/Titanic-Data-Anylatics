# TeamMate → Postgres ingestion

Scheduled, unattended pipeline that full-refreshes TeamMate OData entity sets
into Postgres. Designed to be co-deployed with the web app in the Azure dev slot
and triggered on a CRON schedule.

## Files

| File | Role |
|------|------|
| `teammate_loader.py` | Core loader: loads one entity set (non-blocking staging swap, `@odata.nextLink` paging, retries, reconciliation). |
| `run_full_backfill.py` | Driver: iterates the manifest, loads every entity set, writes the run log, live status JSON, and failures file. CLI. |
| `run_scheduled.py` | Azure entrypoint: advisory-lock single-run guard + SIGTERM graceful shutdown, then calls the driver. |
| `poc_load_one_table.py` | Single-table smoke test. |
| `diag_check_pagination.py` | Validates the `@odata.nextLink` chain against source `$count`. |
| `_common.py` | Shared DB URL, paths, logging setup. |
| `settings.job` | Azure triggered-WebJob CRON schedule (6-field). |

## How a load stays safe

- **Non-blocking atomic swap (default, `staging_swap`).** Every page streams into
  an `UNLOGGED` `<table>__staging` table in short transactions — the live table is
  untouched, so consumers keep reading it the whole time. The swap is one short
  transaction: `DELETE FROM live; INSERT INTO live SELECT * FROM staging;`. Those
  take `ROW EXCLUSIVE`, which does **not** block readers' `ACCESS SHARE`, and MVCC
  means concurrent `SELECT`s see the full old snapshot until commit, then the full
  new one — never a partial load. The live table keeps its identity (OID, FKs,
  indexes, grants, views), unlike a rename-based swap. Cost: temporary table bloat
  reclaimed by autovacuum (an `ANALYZE` is run after the swap).
- **Strict single-transaction (opt-in, `INGEST_LOAD_STRATEGY=single_txn`).**
  `TRUNCATE` + all inserts in one transaction. Atomic and rollback-safe, but holds
  an `ACCESS EXCLUSIVE` lock for the whole load, so readers are blocked. Use only
  when you explicitly want single-transaction semantics over read availability.
- **Server-driven paging.** Follows `@odata.nextLink` (not deep `$skip/$top`),
  eliminating skipped/duplicated rows.
- **Reconciliation.** Compares rows loaded against the source `$count`; a
  mismatch fails the table instead of silently passing.
- **Resilience.** HTTP retries use exponential backoff + jitter and honor
  `Retry-After`/429. A transient DB disconnect retries the whole (idempotent)
  table — the "pause and reconnect" behavior.
- **Observability.** Rotating log file plus a live, atomically-written
  `run_<id>.json` status file you can read **while the run is in flight** to see
  progress and the exact failure reason.

## Environment variables

| Var | Purpose | Default |
|-----|---------|---------|
| `DB_HOST` / `DB_PORT` / `DB_USER` / `DB_PASSWORD` / `DB_NAME` | Postgres connection | (required; `DB_PORT` 5432) |
| `INGEST_DATA_ROOT` | Manifest root | `../sample_data` (relative to package) |
| `INGEST_LOG_DIR` | Run logs | `./logs` |
| `INGEST_STATE_DIR` | Status JSON + failures file | `./state` |
| `INGEST_LOAD_STRATEGY` | `staging_swap` (non-blocking) or `single_txn` | `staging_swap` |

> Secrets: on Azure prefer **Managed Identity / Key Vault** over a password in
> the connection string.

## Running

```bash
# Full run
python -m app.ingestion.run_full_backfill

# Rerun only what failed last time
python -m app.ingestion.run_full_backfill --only-failed

# Specific entities
python -m app.ingestion.run_full_backfill --only Terminologies Issues

# Scheduled entrypoint (what the WebJob invokes)
python -m app.ingestion.run_scheduled
```

`settings.job`'s `"0 0 2 * * *"` runs the WebJob daily at 02:00. Adjust as needed.
Exit code is non-zero whenever any table fails, so Azure marks the run failed.
