import os
import tempfile
from datetime import datetime

import duckdb
import pytest
from fastapi.testclient import TestClient

from api.main import app
from pipelines.model import MarketSignal
from storage.db import connect, upsert_market_signals


@pytest.fixture()
def populated_db(monkeypatch, tmp_path):
    db_path = tmp_path / "signals.duckdb"
    monkeypatch.setenv("MARKET_SIGNALS_DB_PATH", str(db_path))

    conn = connect()
    try:
        records = [
            MarketSignal(
                source="hud_fmr",
                geo_level="county",
                geo_id="49-035",
                geo_name="Salt Lake County, UT",
                observed_at=datetime(2025, 1, 1),
                metric="fmr_2br",
                value=1350.0,
                unit="USD",
            ),
            MarketSignal(
                source="acs",
                geo_level="county",
                geo_id="49-035",
                geo_name="Salt Lake County, UT",
                observed_at=datetime(2025, 1, 1),
                metric="median_household_income",
                value=72000.0,
                unit="USD",
            ),
            MarketSignal(
                source="fred",
                geo_level="county",
                geo_id="49-035",
                geo_name="Salt Lake County, UT",
                observed_at=datetime(2024, 12, 1),
                metric="unemp_rate",
                value=3.4,
                unit="%",
            ),
        ]
        upsert_market_signals(conn, records)
    finally:
        conn.close()

    yield

    if db_path.exists():
        db_path.unlink()


@pytest.fixture()
def client(populated_db):
    with TestClient(app) as test_client:
        yield test_client


def test_signals_json(client):
    response = client.get("/signals", params={"market": "salt_lake_county", "format": "json", "limit": 10})

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 3
    metrics = {item["metric"] for item in payload["items"]}
    assert {"fmr_2br", "median_household_income", "unemp_rate"} <= metrics


def test_signals_csv(client):
    response = client.get("/signals", params={"market": "salt_lake_county", "format": "csv"})

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    body = response.content.decode()
    assert "fmr_2br" in body
    assert "median_household_income" in body


def test_signals_parquet(client):
    response = client.get("/signals", params={"market": "salt_lake_county", "format": "parquet"})

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/vnd.apache.parquet")

    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        tmp.write(response.content)
        tmp.flush()
        con = duckdb.connect()
        try:
            count = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [tmp.name]).fetchone()[0]
        finally:
            con.close()
    assert count == 3
