"""Static configuration for target markets and related series metadata."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class MarketConfig:
    """Configuration describing how to ingest metrics for a market."""

    key: str
    geo_level: str
    geo_id: str
    geo_name: str
    year: int
    fred_series_id: str | None = None
    fred_metric: str = "unemp_rate"
    fred_unit: str = "%"


TARGET_MARKETS: tuple[MarketConfig, ...] = (
    MarketConfig(
        key="salt_lake_county",
        geo_level="county",
        geo_id="49-035",
        geo_name="Salt Lake County, UT",
        year=2025,
        fred_series_id="LAUCN490350000000003A",
    ),
    MarketConfig(
        key="maricopa_county",
        geo_level="county",
        geo_id="04-013",
        geo_name="Maricopa County, AZ",
        year=2025,
        fred_series_id="LAUCN040130000000003A",
    ),
    MarketConfig(
        key="travis_county",
        geo_level="county",
        geo_id="48-453",
        geo_name="Travis County, TX",
        year=2025,
        fred_series_id="LAUCN484530000000003A",
    ),
    MarketConfig(
        key="king_county",
        geo_level="county",
        geo_id="53-033",
        geo_name="King County, WA",
        year=2025,
        fred_series_id="LAUCN530330000000003A",
    ),
)


def get_market_by_key(key: str) -> MarketConfig | None:
    for market in TARGET_MARKETS:
        if market.key == key:
            return market
    return None


def iter_markets(keys: Iterable[str] | None = None) -> Iterable[MarketConfig]:
    if keys is None:
        return TARGET_MARKETS
    selected = []
    for key in keys:
        market = get_market_by_key(key)
        if market:
            selected.append(market)
    return tuple(selected)


__all__ = ["MarketConfig", "TARGET_MARKETS", "get_market_by_key", "iter_markets"]
