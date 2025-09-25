# CRE Market Signals

Unified pipeline for ingesting HUD FMR, Census ACS, and FRED metrics; normalizing them into a single DuckDB store; and serving the results through a FastAPI backend and Next.js dashboard.

## Prerequisites

- Python 3.12+
- Node.js 20+
- DuckDB (installed automatically via Python package)

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
cp web/.env.local web/.env.local.example  # adjust as needed
```

Populate `.env` with API keys (HUD, FRED, optional Census) and configure defaults such as `DEFAULT_GEO`.

## Running the Pipeline

1. **Ingest data** into DuckDB using the job runner:
   ```bash
   python -m jobs load-all
   ```
   - `python -m jobs list-markets` enumerates seeded markets.
   - Use `--markets key1,key2` to scope a run or set env vars (`DEFAULT_GEO`, `FRED_SERIES_ID`).

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

## Docker

Build and run the API and web dashboard with Docker Compose:

```bash
docker compose up --build
```

- `Dockerfile.api` builds the FastAPI service and seeds dependencies from `requirements.txt`.
- `Dockerfile.web` produces a static Next.js build served via `next start`.
- `docker-compose.yml` wires both services; the API persists `market_signals.duckdb` in a volume.

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

## Roadmap

- Add automated scheduling (cron/Airflow) for `jobs.load_all`.
- Extend ingestors for Overpass/Walk Score or other sources.
- Expand test coverage with fixture playback for API responses.
