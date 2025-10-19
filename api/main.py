"""FastAPI service exposing normalized market signals in multiple formats."""

from __future__ import annotations

import os
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Sequence

import duckdb
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from dotenv import load_dotenv

from jobs.config import MarketConfig, get_market_by_key
from pipelines.model import MarketSignal
from storage.db import MARKET_SIGNALS_TABLE, connect, fetch_market_signals
from storage.exports import export_to_csv, export_to_parquet

DEFAULT_LIMIT = 200
MAX_LIMIT = 2000
ALLOWED_FORMATS = {"json", "csv", "parquet"}
load_dotenv()


@asynccontextmanager
async def lifespan(_: FastAPI):
    conn = connect()
    conn.close()
    yield


app = FastAPI(title="CRE Market Signals API", version="0.1.0", lifespan=lifespan)


def _configure_cors() -> None:
    raw_origins = os.getenv("API_CORS_ORIGINS", "*")
    origins = [origin.strip() for origin in raw_origins.split(",") if origin.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins or ["*"],
        allow_methods=["*"],
        allow_headers=["*"],
        allow_credentials=True,
    )


_configure_cors()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
def _build_filters(
    *,
    market: str | None,
    geo_level: str | None,
    geo_id: str | None,
    metric: str | None,
) -> tuple[str | None, list[Any]]:
    filters: list[str] = []
    params: list[Any] = []

    resolved_market: MarketConfig | None = None
    if market:
        resolved_market = get_market_by_key(market)
        if not resolved_market:
            raise HTTPException(status_code=404, detail=f"Unknown market key '{market}'")
        geo_level = geo_level or resolved_market.geo_level
        geo_id = geo_id or resolved_market.geo_id

    if geo_level:
        filters.append("geo_level = ?")
        params.append(geo_level)
    if geo_id:
        filters.append("geo_id = ?")
        params.append(geo_id)
    if metric:
        filters.append("metric = ?")
        params.append(metric)

    if not filters:
        return None, params

    return " AND ".join(filters), params


def _build_query(where: str | None, limit: int) -> str:
    sql = f"SELECT * FROM {MARKET_SIGNALS_TABLE}"
    if where:
        sql += f" WHERE {where}"
    sql += " ORDER BY observed_at DESC"
    sql += f" LIMIT {limit}"
    return sql


def _serialize_signals(signals: Sequence[MarketSignal]) -> list[dict[str, Any]]:
    return [signal.model_dump(mode="json") for signal in signals]


@app.get("/signals")
def get_signals(
    background_tasks: BackgroundTasks,
    format: str = Query("json", description="Response format: json, csv, or parquet"),
    market: str | None = Query(None, description="Preconfigured market key"),
    geo_level: str | None = Query(None, description="Geographic aggregation level"),
    geo_id: str | None = Query(None, description="Geography identifier (FIPS, CBSA, etc.)"),
    metric: str | None = Query(None, description="Metric identifier to filter"),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=MAX_LIMIT, description="Maximum records returned"),
):
    fmt = format.lower()
    if fmt not in ALLOWED_FORMATS:
        raise HTTPException(status_code=400, detail=f"Unsupported format '{format}'.")

    where, params = _build_filters(
        market=market,
        geo_level=geo_level,
        geo_id=geo_id,
        metric=metric,
    )

    query = _build_query(where, limit)

    conn = connect(read_only=True)
    try:
        if fmt == "json":
            signals = fetch_market_signals(conn, where=where, params=params, limit=limit)
            payload = {
                "count": len(signals),
                "items": _serialize_signals(signals),
            }
            return JSONResponse(content=payload)

        suffix = ".csv" if fmt == "csv" else ".parquet"
        media_type = "text/csv" if fmt == "csv" else "application/vnd.apache.parquet"
        filename = f"signals{suffix}"
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            dest = Path(tmp.name)

        if fmt == "csv":
            export_to_csv(conn, dest, query=query, params=params)
        else:
            export_to_parquet(conn, dest, query=query, params=params)

        def _cleanup(path: Path) -> None:
            try:
                path.unlink()
            except FileNotFoundError:
                pass

        background_tasks.add_task(_cleanup, dest)
        return FileResponse(dest, media_type=media_type, filename=filename, background=background_tasks)
    except duckdb.Error as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=500, detail="Database query failed") from exc
    finally:
        conn.close()
