"""DuckDB persistence utilities for normalized market signals."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Iterable, Sequence

import duckdb

from pipelines.model import MarketSignal

DB_ENV_VAR = "MARKET_SIGNALS_DB_PATH"
DEFAULT_DB_PATH = Path("data/market_signals.duckdb")

MARKET_SIGNALS_TABLE = "market_signals"


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def get_database_path(override: str | os.PathLike[str] | None = None) -> Path:
    """Resolve the DuckDB file path from an explicit override or environment variable."""

    if override is not None:
        return Path(override)
    env_value = os.getenv(DB_ENV_VAR)
    if env_value:
        return Path(env_value)
    return DEFAULT_DB_PATH


def connect(
    path: str | os.PathLike[str] | None = None,
    *,
    read_only: bool = False,
    ensure: bool = True,
) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection, optionally ensuring schema availability."""

    db_path = get_database_path(path)
    if not read_only:
        _ensure_parent_dir(db_path)
    conn = duckdb.connect(str(db_path), read_only=read_only)
    if ensure and not read_only:
        ensure_market_signals_table(conn)
    return conn


def ensure_market_signals_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the canonical storage table if it does not already exist."""

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {MARKET_SIGNALS_TABLE} (
            source TEXT NOT NULL,
            geo_level TEXT NOT NULL,
            geo_id TEXT NOT NULL,
            geo_name TEXT NOT NULL,
            observed_at TIMESTAMP NOT NULL,
            metric TEXT NOT NULL,
            value DOUBLE,
            unit TEXT NOT NULL,
            raw_payload JSON,
            PRIMARY KEY (source, geo_level, geo_id, observed_at, metric)
        )
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{MARKET_SIGNALS_TABLE}_geo
        ON {MARKET_SIGNALS_TABLE} (geo_level, geo_id)
        """
    )
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{MARKET_SIGNALS_TABLE}_metric
        ON {MARKET_SIGNALS_TABLE} (metric)
        """
    )


def _serialize_signal(signal: MarketSignal) -> tuple:
    data = signal.dict()
    raw_payload = data.get("raw_payload")
    return (
        data["source"],
        data["geo_level"],
        data["geo_id"],
        data["geo_name"],
        data["observed_at"],
        data["metric"],
        data["value"],
        data["unit"],
        json.dumps(raw_payload) if raw_payload is not None else None,
    )


def upsert_market_signals(
    conn: duckdb.DuckDBPyConnection, signals: Iterable[MarketSignal]
) -> int:
    """Insert or replace a batch of ``MarketSignal`` records.

    Returns
    -------
    int
        Number of records written to the database.
    """

    serialized = [_serialize_signal(signal) for signal in signals]
    if not serialized:
        return 0

    conn.executemany(
        f"""
        INSERT OR REPLACE INTO {MARKET_SIGNALS_TABLE} (
            source,
            geo_level,
            geo_id,
            geo_name,
            observed_at,
            metric,
            value,
            unit,
            raw_payload
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        serialized,
    )
    return len(serialized)


def fetch_market_signals(
    conn: duckdb.DuckDBPyConnection,
    *,
    where: str | None = None,
    params: Sequence[object] | None = None,
    limit: int | None = None,
) -> list[MarketSignal]:
    """Query stored records and reconstruct ``MarketSignal`` models."""

    sql = f"SELECT * FROM {MARKET_SIGNALS_TABLE}"
    if where:
        sql += f" WHERE {where}"
    if limit is not None:
        sql += f" LIMIT {limit}"
    cursor = conn.execute(sql, params or [])
    results: list[MarketSignal] = []
    for row in cursor.fetchall():
        payload = row[8]
        results.append(
            MarketSignal(
                source=row[0],
                geo_level=row[1],
                geo_id=row[2],
                geo_name=row[3],
                observed_at=row[4],
                metric=row[5],
                value=row[6],
                unit=row[7],
                raw_payload=json.loads(payload) if isinstance(payload, str) else payload,
            )
        )
    return results


__all__ = [
    "connect",
    "ensure_market_signals_table",
    "upsert_market_signals",
    "fetch_market_signals",
    "MARKET_SIGNALS_TABLE",
    "get_database_path",
]
