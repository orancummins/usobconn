# usobconn

Monarch connection-status tracker running in Docker with PostgreSQL.

## Run App

Start normally:

```bash
./run.sh
```

Start with a fresh PostgreSQL database (destructive):

```bash
./run.sh --clean
```

`--wipe-db` is accepted as an alias for `--clean`.

## Recommended Flow

1. Start app (`./run.sh` or `./run.sh --clean` for a fresh DB).
2. Run scrape/import flow (fast DB save path).
3. Logos are served from local cache in `instance/logos`.
4. Run logo refresh separately when needed.

```mermaid
flowchart TD
    A[Start App] --> B{Clean start?}
    B -- Yes --> C[./run.sh --clean]
    B -- No --> D[./run.sh]
    C --> E[PostgreSQL ready]
    D --> E

    E --> F[Start Scrape / Import]
    F --> G[Fetch institution data from ISS API]
    G --> H[Bulk insert to PostgreSQL in batches]
    H --> I[Commit session and rows]
    I --> J[Light logo cache step\n(base64/data-image only)]
    J --> K[UI reads logos from /api/logo/*\nserved from instance/logos]

    E --> L[Optional: Separate logo refresh]
    L --> M[refresh_logos_cache.py or POST /api/logos/refresh]
    M --> N[Fetch remote logos if missing/forced]
    N --> K
```

Why it is fast now:
- The scrape "saving" phase is mostly batched PostgreSQL inserts.
- Remote logo fetching is decoupled into a separate refresh path.
- Cached logos are served from disk, so normal runs avoid logo network latency.

## Database Utilities

These scripts run from the host and execute inside the `app` container.

### 1) Wipe PostgreSQL (standalone)

Use this when you want a clean database before importing or scraping again.

```bash
./scripts/wipe_postgres_db_from_app_container.sh
```

Notes:
- This is destructive: all rows in all app tables are deleted.
- Sequences are reset.

### 2) Import SQLite into PostgreSQL (standalone)

After wiping (or anytime you want to replace app-table data from SQLite):

```bash
./scripts/import_sqlite_from_app_container.sh
```

Optional variables:

```bash
BATCH_SIZE=2000 SQLITE_PATH=/app/instance/monarch.db ./scripts/import_sqlite_from_app_container.sh
```

## Logo Cache Refresh (separate from scrape save)

Refresh logo files without re-running database import/scrape:

```bash
docker compose exec -T app python /app/scripts/refresh_logos_cache.py
```

Force-refresh existing logo files:

```bash
docker compose exec -T app python /app/scripts/refresh_logos_cache.py --force
```

Or via API:

```bash
curl -X POST http://localhost:9093/api/logos/refresh
```

```bash
curl -X POST http://localhost:9093/api/logos/refresh \
  -H 'Content-Type: application/json' \
  -d '{"force": true}'
```
