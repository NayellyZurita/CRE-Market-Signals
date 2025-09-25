"""Census American Community Survey ingestor.

Retrieves ACS statistics for configured variables and converts them into
``MarketSignal`` entries.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Mapping

from pipelines.common import fetch_json
from pipelines.model import MarketSignal

ACS_BASE_URL = "https://api.census.gov/data"
ACS_DEFAULT_DATASET = "acs/acs5"

# Default set of ACS variables commonly used for market analysis.
ACS_DEFAULT_VARIABLES: Mapping[str, tuple[str, str]] = {
    "B01003_001E": ("population_total", "count"),
    "B19013_001E": ("median_household_income", "USD"),
    "B25077_001E": ("median_home_value", "USD"),
    "B25058_001E": ("median_gross_rent", "USD"),
}

_SENTINEL_STRINGS = {"", "N/A", "NA", "null", "Null", "-"}
_SENTINEL_NUMBERS = {"-666666666", "-888888888", "-999999999"}


def _resolve_api_key(api_key: str | None) -> str | None:
    return api_key or os.getenv("CENSUS_API_KEY")


def _coerce_numeric(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    if value in _SENTINEL_STRINGS or value in _SENTINEL_NUMBERS:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _build_geo_params(geo_level: str, state_fips: str, county_fips: str | None) -> Mapping[str, str]:
    if geo_level == "county":
        if not county_fips:
            raise ValueError("county_fips is required when geo_level='county'.")
        return {
            "for": f"county:{county_fips}",
            "in": f"state:{state_fips}",
        }
    if geo_level == "state":
        return {"for": f"state:{state_fips}"}
    raise ValueError(f"Unsupported geo_level '{geo_level}'.")


def _build_geo_metadata(geo_level: str, state_fips: str, county_fips: str | None) -> tuple[str, str]:
    if geo_level == "county":
        if not county_fips:
            raise ValueError("county_fips is required when geo_level='county'.")
        return "county", f"{state_fips}-{county_fips}"
    if geo_level == "state":
        return "state", state_fips
    raise ValueError(f"Unsupported geo_level '{geo_level}'.")


async def fetch_acs(
    *,
    year: int,
    state_fips: str,
    county_fips: str | None = None,
    geo_level: str = "county",
    dataset: str = ACS_DEFAULT_DATASET,
    variables: Mapping[str, tuple[str, str]] = ACS_DEFAULT_VARIABLES,
    api_key: str | None = None,
    extra_params: Mapping[str, Any] | None = None,
) -> list[MarketSignal]:
    """Fetch ACS data and normalize it into ``MarketSignal`` objects."""

    if not variables:
        return []

    headers = ["NAME", *variables.keys()]
    params: dict[str, Any] = {
        "get": ",".join(headers),
    }
    params.update(_build_geo_params(geo_level, state_fips, county_fips))

    resolved_key = _resolve_api_key(api_key)
    if resolved_key:
        params["key"] = resolved_key
    if extra_params:
        params.update(extra_params)

    url = f"{ACS_BASE_URL}/{year}/{dataset}"
    payload = await fetch_json(url, params=params)

    if not isinstance(payload, list) or not payload:
        return []

    header_row = payload[0]
    data_rows = payload[1:]

    if not isinstance(header_row, list):
        return []

    observed_at = datetime(year, 1, 1)
    geo_level_normalized, geo_id = _build_geo_metadata(geo_level, state_fips, county_fips)

    signals: list[MarketSignal] = []
    for row in data_rows:
        if not isinstance(row, list) or len(row) != len(header_row):
            continue
        row_dict = dict(zip(header_row, row, strict=False))
        geo_name = row_dict.get("NAME") or geo_id

        for variable, (metric, unit) in variables.items():
            value = _coerce_numeric(row_dict.get(variable))
            if value is None:
                continue
            signals.append(
                MarketSignal(
                    source="acs",
                    geo_level=geo_level_normalized,
                    geo_id=geo_id,
                    geo_name=geo_name,
                    observed_at=observed_at,
                    metric=metric,
                    value=value,
                    unit=unit,
                    raw_payload={
                        "variable": variable,
                        "value": row_dict.get(variable),
                        "raw": row_dict,
                    },
                )
            )

    return signals


__all__ = ["fetch_acs", "ACS_BASE_URL", "ACS_DEFAULT_DATASET", "ACS_DEFAULT_VARIABLES"]
