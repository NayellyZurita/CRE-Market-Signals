"""Export helpers for data persisted inside DuckDB."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

import duckdb

from storage.db import MARKET_SIGNALS_TABLE


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _default_query() -> str:
    return f"SELECT * FROM {MARKET_SIGNALS_TABLE} ORDER BY observed_at"


def export_to_csv(
    conn: duckdb.DuckDBPyConnection,
    destination: str | Path,
    *,
    query: str | None = None,
    params: Sequence[Any] | None = None,
    include_header: bool = True,
) -> Path:
    """Materialize query results into a CSV file using DuckDB's COPY command."""

    dest_path = Path(destination)
    _ensure_parent(dest_path)
    sql = query or _default_query()
    sanitized_path = str(dest_path).replace("'", "''")
    copy_sql = (
        f"COPY ({sql}) TO '{sanitized_path}'"
        f" (FORMAT CSV, HEADER {'TRUE' if include_header else 'FALSE'})"
    )
    conn.execute(copy_sql, params or [])
    return dest_path


def export_to_parquet(
    conn: duckdb.DuckDBPyConnection,
    destination: str | Path,
    *,
    query: str | None = None,
    params: Sequence[Any] | None = None,
) -> Path:
    """Export query results to a Parquet file."""

    dest_path = Path(destination)
    _ensure_parent(dest_path)
    sql = query or _default_query()
    sanitized_path = str(dest_path).replace("'", "''")
    copy_sql = (
        f"COPY ({sql}) TO '{sanitized_path}'"
        " (FORMAT PARQUET)"
    )
    conn.execute(copy_sql, params or [])
    return dest_path


def fetch_dataframe(
    conn: duckdb.DuckDBPyConnection,
    *,
    query: str | None = None,
    params: Sequence[Any] | None = None,
):
    """Return query results as a pandas DataFrame if pandas is available."""

    sql = query or _default_query()
    try:
        return conn.execute(sql, params or []).df()
    except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "pandas is required to fetch results as a DataFrame. Install pandas or "
            "use export_to_csv/export_to_parquet instead."
        ) from exc


__all__ = ["export_to_csv", "export_to_parquet", "fetch_dataframe"]
