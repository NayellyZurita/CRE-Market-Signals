# CRE Market Signals

Unified pipeline for ingesting HUD FMR, Census ACS, and FRED metrics; normalizing them into a single DuckDB store; and serving the results through a FastAPI backend and Next.js dashboard.


<img width="1536" height="1024" alt="CRE-Arquitecture-Diagram" src="https://github.com/user-attachments/assets/107cdbc7-5ceb-44f6-b149-f7e5049f5334" />

## Prerequisites
- Python 3.12+
- Node.js 20+
- DuckDB (installed automatically via Python package)
- Docker & Docker Compose (for orchestrated stack / Airflow)

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
npm --prefix web install
```

Copy environment templates:

```bash
cp .env.example .env
```

Create `web/.env.local` (or wire your own config) with at least:

```
NEXT_PUBLIC_API_BASE_URL=http://localhost:8000
NEXT_PUBLIC_DEFAULT_MARKET=salt_lake_county
```

Populate `.env` with API keys (HUD, FRED, optional Census) and configure defaults such as `DEFAULT_GEO`, `DEFAULT_START_YEAR`, and `DEFAULT_END_YEAR`. `jobs/` and the API load this file automatically via `python-dotenv`.

For Airflow you also need to set:

- `COMPOSE_PROJECT_NAME` – used to derive the shared Docker network name.
- `AIRFLOW_UID` – your host UID for file ownership (50000 works on macOS/Linux).
- `AIRFLOW_ADMIN_USERNAME` / `AIRFLOW_ADMIN_PASSWORD` – web UI credentials.
- `AIRFLOW_DB_PASSWORD` – Postgres password for the Airflow metadata database.

## Running the Pipeline

1. **Ingest data** into DuckDB using the job runner:
   ```bash
   python -m jobs load-all
   ```
   - `python -m jobs list-markets` enumerates seeded markets.
   - Use `--markets key1,key2` to scope a run or set env vars (`DEFAULT_GEO`, `DEFAULT_START_YEAR`, `DEFAULT_END_YEAR`, `FRED_SERIES_ID`).
   - HUD/ACS ingestion loops through every year in `[DEFAULT_START_YEAR, DEFAULT_END_YEAR]`; ACS currently publishes through 2023, so later years log warnings and are skipped until available. FRED honours `FRED_OBSERVATION_START` / `FRED_OBSERVATION_END` when provided.

2. **Serve the API** for normalized access:
   ```bash
   uvicorn api.main:app --reload
   ```
   - `/signals` endpoint supports `format=json|csv|parquet`, filter params (`market`, `geo_level`, `metric`), and returns up to `limit` records.
   - `/health` provides a basic readiness probe.

3. **Launch the Next.js dashboard**:
   ```bash
   npm --prefix web run dev
   ```
   - Reads `NEXT_PUBLIC_API_BASE_URL` to call the API.
   - Shows download buttons and a quick chart (Recharts) for the preferred metric (defaults to `fmr_2br`).

## Testing

```bash
pytest -q
npm --prefix web run lint
```

Tests cover Pydantic schema validation and API format exports (JSON/CSV/Parquet). A `tests/conftest.py` fixture ensures the project root is importable.

## Inspecting Data

Use DuckDB to review what was ingested:

```bash
duckdb data/market_signals.duckdb
SELECT source, metric, COUNT(*) FROM market_signals GROUP BY 1,2 ORDER BY 1,2;
```

Expect five HUD rent metrics (`fmr_0br`…`fmr_4br`) per year, four ACS metrics for available years, and a monthly FRED unemployment series across the configured observation window.

## Docker

Build and run the API and web dashboard with Docker Compose:

```bash
docker compose up --build
```

- `Dockerfile.api` builds the FastAPI service and seeds dependencies from `requirements.txt`.
- `Dockerfile.web` produces a static Next.js build served via `next start`.
- `docker-compose.yml` wires the stack: API, web, Postgres, and Airflow services all share a named volume (`market_signals_data`) so the DuckDB file stays consistent.

## Airflow Orchestration

1. Ensure `.env` contains the Airflow variables mentioned above (admin credentials, UID, project name, API tokens).
2. Bootstrap the Airflow metadata database:
   ```bash
   docker compose up airflow-init
   ```
3. Start the full stack (API, web, Postgres, Airflow webserver & scheduler):
   ```bash
   docker compose up --build
   ```
4. Open the Airflow UI at <http://localhost:8080> and log in with `AIRFLOW_ADMIN_USERNAME` / `AIRFLOW_ADMIN_PASSWORD`.
5. Locate the `load_all_daily` DAG, unpause it, and trigger a manual run to confirm `jobs.load_all` executes successfully.
6. The DAG performs basic data-quality checks (row counts per source) and records a `load_status` timestamp inside DuckDB. Inspect task logs in Airflow for run details.

Tip: Airflow uses the DockerOperator to run the same API image that powers the CLI, so HUD/FRED tokens and the DuckDB volume are shared automatically. Leave new DAGs paused in development to avoid automatic runs, and adjust `DEFAULT_START_YEAR` / `DEFAULT_END_YEAR` in `.env` when you want to backfill additional years.
Only the API service and DockerOperator task mount the named volume `market_signals_data` at `/app/data`; the Airflow webserver/scheduler do not need that volume.

## Project Structure

```
├── api/               # FastAPI application
├── jobs/              # Batch jobs (loaders, CLI entrypoint)
├── pipelines/         # Source-specific ingestors & schema
├── storage/           # DuckDB helpers and export utilities
├── tests/             # Pytest suites
└── web/               # Next.js dashboard (Recharts, downloads)
```

## Troubleshooting

- **API returning 500**: Ensure the DuckDB file contains data (`python -m jobs load-all`) and that `.env` points to correct DB path. Startup hook pre-creates the schema.
- **Frontend “Failed to fetch”**: Confirm `uvicorn` is running on the host configured via `NEXT_PUBLIC_API_BASE_URL`.
- **CORS issues**: Set `API_CORS_ORIGINS` (comma-separated) in `.env` before launching the API.
- **Airflow warnings about missing tokens**: confirm the `.env` file mounted in Compose includes `HUD_TOKEN`, `FRED_API_KEY`, and `CENSUS_API_KEY` so the DockerOperator inherits them.

## Roadmap

- Add automated scheduling (cron/Airflow) for `jobs.load_all`.
- Extend ingestors for Overpass/Walk Score or other sources.
- Expand test coverage with fixture playback for API responses.
