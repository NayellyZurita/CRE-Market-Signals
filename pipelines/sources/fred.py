"""St. Louis Fed (FRED) ingestor utilities."""

from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from pipelines.common import fetch_json
from pipelines.model import MarketSignal

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"


@dataclass(frozen=True)
class FredSeriesConfig:
    """Metadata needed to normalize a FRED series into ``MarketSignal`` rows."""

    series_id: str
    metric: str
    unit: str
    geo_level: str
    geo_id: str
    geo_name: str


_SENTINEL_VALUES = {".", "NA", "N/A", ""}

logger = logging.getLogger(__name__)


def _resolve_api_key(api_key: str | None) -> str | None:
    resolved = api_key or os.getenv("FRED_API_KEY")
    if not resolved:
        logger.warning(
            "FRED API key missing. Skipping FRED fetch. Set FRED_API_KEY or pass api_key explicitly."
        )
    return resolved


def _parse_observation_date(raw_date: str) -> datetime | None:
    try:
        return datetime.fromisoformat(raw_date)
    except ValueError:
        try:
            return datetime.strptime(raw_date, "%Y-%m-%d")
        except ValueError:
            return None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
    elif isinstance(value, str):
        stripped = value.strip()
        if stripped in _SENTINEL_VALUES:
            return None
        try:
            numeric = float(stripped)
        except ValueError:
            return None
    else:
        return None

    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric


async def fetch_fred_series(
    config: FredSeriesConfig,
    *,
    api_key: str | None = None,
    observation_start: str | None = None,
    observation_end: str | None = None,
    params: Mapping[str, Any] | None = None,
) -> list[MarketSignal]:
    """Fetch observations for a configured FRED series and normalize them."""

    resolved_key = _resolve_api_key(api_key)
    if not resolved_key:
        return []
    request_params: dict[str, Any] = {
        "series_id": config.series_id,
        "api_key": resolved_key,
        "file_type": "json",
    }
    if observation_start:
        request_params["observation_start"] = observation_start
    if observation_end:
        request_params["observation_end"] = observation_end
    if params:
        request_params.update(params)

    payload = await fetch_json(FRED_BASE_URL, params=request_params)

    observations = payload.get("observations") if isinstance(payload, Mapping) else None
    if not isinstance(observations, list):
        return []

    signals: list[MarketSignal] = []
    for obs in observations:
        if not isinstance(obs, Mapping):
            continue
        observed_at = _parse_observation_date(str(obs.get("date", "")))
        if not observed_at:
            continue
        value = _coerce_float(obs.get("value"))
        if value is None:
            continue
        signals.append(
            MarketSignal(
                source="fred",
                geo_level=config.geo_level,
                geo_id=config.geo_id,
                geo_name=config.geo_name,
                observed_at=observed_at,
                metric=config.metric,
                value=value,
                unit=config.unit,
                raw_payload=obs,
            )
        )

    return signals


__all__ = ["FredSeriesConfig", "fetch_fred_series", "FRED_BASE_URL"]
