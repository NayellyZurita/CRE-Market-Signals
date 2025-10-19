"""End-to-end job that fetches all configured market signals and persists them."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Iterable

from dotenv import load_dotenv

from pipelines.sources.acs import fetch_acs
from pipelines.sources.fred import FredSeriesConfig, fetch_fred_series
from pipelines.sources.hud_fmr import fetch_hud_fmr
from pipelines.model import MarketSignal
from storage.db import connect, upsert_market_signals
from jobs.config import MarketConfig, TARGET_MARKETS, iter_markets

load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_FRED_SERIES_ID_ENV = "FRED_SERIES_ID"
DEFAULT_FRED_METRIC_ENV = "FRED_SERIES_METRIC"
DEFAULT_FRED_UNIT_ENV = "FRED_SERIES_UNIT"


def _parse_geo(raw: str) -> tuple[str, str]:
    try:
        level, identifier = raw.split(":", 1)
    except ValueError as exc:  # pragma: no cover - defensive
        raise ValueError(
            "DEFAULT_GEO must use the format '<level>:<id>' (e.g. 'county:49-035')."
        ) from exc
    return level.strip(), identifier.strip()


def _split_fips(geo_id: str) -> tuple[str, str | None]:
    if "-" in geo_id:
        state, county = geo_id.split("-", 1)
        return state, county
    return geo_id, None


async def _gather_signals_for_market(market: MarketConfig) -> list[MarketSignal]:
    state_fips, county_fips = _split_fips(market.geo_id)

    collected: list[MarketSignal] = []

    for year in market.years:
        collected.extend(
            await fetch_hud_fmr(
                entity_id=market.geo_id,
                geo_level=market.geo_level,
                year=year,
            )
        )

        collected.extend(
            await fetch_acs(
                year=year,
                state_fips=state_fips,
                county_fips=county_fips,
                geo_level=market.geo_level,
            )
        )

    fred_signals: list[MarketSignal] = []
    if market.fred_series_id:
        fred_config = FredSeriesConfig(
            series_id=market.fred_series_id,
            metric=market.fred_metric,
            unit=market.fred_unit,
            geo_level=market.geo_level,
            geo_id=market.geo_id,
            geo_name=market.geo_name,
        )
        fred_signals = await fetch_fred_series(
            fred_config,
            observation_start=market.fred_observation_start
            or f"{market.start_year}-01-01",
            observation_end=market.fred_observation_end
            or f"{market.end_year}-12-31",
        )
    else:
        logger.info("Skipping FRED load for %s (no series configured).", market.key)

    return [*collected, *fred_signals]


def _markets_from_env() -> list[MarketConfig] | None:
    default_geo = os.getenv("DEFAULT_GEO")
    if not default_geo:
        return None

    geo_level, geo_id = _parse_geo(default_geo)
    start_year = int(
        os.getenv("DEFAULT_START_YEAR")
        or os.getenv("DEFAULT_YEAR", "2025")
    )
    end_year = int(os.getenv("DEFAULT_END_YEAR", str(start_year)))
    geo_name = os.getenv("DEFAULT_GEO_NAME", geo_id)
    fred_series_id = os.getenv(DEFAULT_FRED_SERIES_ID_ENV)
    fred_metric = os.getenv(DEFAULT_FRED_METRIC_ENV, "unemp_rate")
    fred_unit = os.getenv(DEFAULT_FRED_UNIT_ENV, "%")
    fred_observation_start = os.getenv("FRED_OBSERVATION_START")
    fred_observation_end = os.getenv("FRED_OBSERVATION_END")

    return [
        MarketConfig(
            key=os.getenv("DEFAULT_MARKET_KEY", "env_default"),
            geo_level=geo_level,
            geo_id=geo_id,
            geo_name=geo_name,
            start_year=start_year,
            end_year=end_year,
            fred_series_id=fred_series_id,
            fred_metric=fred_metric,
            fred_unit=fred_unit,
            fred_observation_start=fred_observation_start,
            fred_observation_end=fred_observation_end,
        )
    ]


def _resolve_markets() -> tuple[MarketConfig, ...]:
    env_markets = _markets_from_env()
    if env_markets:
        return tuple(env_markets)

    requested = os.getenv("LOAD_MARKETS")
    if requested:
        keys = [key.strip() for key in requested.split(",") if key.strip()]
        selected = tuple(iter_markets(keys))
        if selected:
            return selected
        logger.warning(
            "LOAD_MARKETS=%s did not match any configured markets; falling back to defaults.",
            requested,
        )

    return TARGET_MARKETS


async def load_all_async(markets: Iterable[MarketConfig] | None = None) -> int:
    """Fetch all available market signals and persist them into DuckDB."""

    markets = tuple(markets) if markets is not None else _resolve_markets()
    total_written = 0
    conn = connect()
    try:
        for market in markets:
            logger.info("Fetching signals for %s (%s)...", market.geo_name, market.key)
            signals = await _gather_signals_for_market(market)
            if not signals:
                logger.warning("No signals fetched for %s; skipping write.", market.key)
                continue
            written = upsert_market_signals(conn, signals)
            logger.info(
                "Persisted %s records for %s.", written, market.key
            )
            total_written += written
        return total_written
    finally:
        conn.close()


def main(markets: Iterable[MarketConfig] | None = None) -> int:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    written = asyncio.run(load_all_async(markets))
    logger.info("Load-all job finished (records written=%s).", written)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
