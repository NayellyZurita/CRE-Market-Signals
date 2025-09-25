"""HUD Fair Market Rent ingestor.

Turns HUD API responses into normalized ``MarketSignal`` records that downstream
pipelines can persist or serve.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Iterable, Mapping

from pipelines.common import fetch_json
from pipelines.model import MarketSignal

HUD_FMR_BASE_URL = "https://www.huduser.gov/hudapi/public/fmr/data"

# Mapping of HUD field names to canonical metric identifiers and units.
HUD_FMR_FIELDS: Mapping[str, tuple[str, str]] = {
    "fmr_0br": ("FMR_0", "USD"),
    "fmr_1br": ("FMR_1", "USD"),
    "fmr_2br": ("FMR_2", "USD"),
    "fmr_3br": ("FMR_3", "USD"),
    "fmr_4br": ("FMR_4", "USD"),
}


def _resolve_token(token: str | None) -> str:
    resolved = token or os.getenv("HUD_TOKEN")
    if not resolved:
        raise RuntimeError(
            "HUD API token missing. Provide it explicitly or set HUD_TOKEN environment variable."
        )
    return resolved


def _iter_fmr_values(record: Mapping[str, Any]) -> Iterable[tuple[str, str, float]]:
    for metric, (field_name, unit) in HUD_FMR_FIELDS.items():
        raw_value = record.get(field_name)
        if raw_value in (None, "", "NA", "N/A"):
            continue
        try:
            yield metric, unit, float(raw_value)
        except (TypeError, ValueError):
            continue


def _extract_geo_name(record: Mapping[str, Any]) -> str | None:
    parts: list[str] = []
    for key in ("county", "cbsa_name", "county_name", "name"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            parts.append(value.strip())
    state = record.get("state")
    if isinstance(state, str) and state.strip():
        parts.append(state.strip())
    if parts:
        return ", ".join(dict.fromkeys(parts))  # maintain order, remove duplicates
    return None


async def fetch_hud_fmr(
    entity_id: str,
    *,
    geo_level: str,
    year: int,
    token: str | None = None,
    params: Mapping[str, Any] | None = None,
) -> list[MarketSignal]:
    """Fetch HUD Fair Market Rent data for a geography and normalize it.

    Parameters
    ----------
    entity_id:
        Identifier HUD expects in the URL (county FIPS, CBSA code, etc.). This becomes
        the canonical ``geo_id`` in the resulting signals.
    geo_level:
        The geography granularity, such as ``"county"`` or ``"cbsa"``.
    year:
        Fiscal year to request from HUD.
    token:
        Optional HUD API bearer token. Falls back to ``HUD_TOKEN`` environment variable.
    params:
        Additional query parameters forwarded to the API.
    """

    resolved_token = _resolve_token(token)
    request_params = {"year": year}
    if params:
        request_params.update(params)

    payload = await fetch_json(
        f"{HUD_FMR_BASE_URL}/{entity_id}",
        headers={"Authorization": f"Bearer {resolved_token}"},
        params=request_params,
    )

    records: list[Mapping[str, Any]]
    if isinstance(payload, Mapping):
        candidate = payload.get("data", payload)
        if isinstance(candidate, Mapping):
            records = [candidate]
        elif isinstance(candidate, list):
            records = [rec for rec in candidate if isinstance(rec, Mapping)]
        else:
            records = []
    elif isinstance(payload, list):
        records = [rec for rec in payload if isinstance(rec, Mapping)]
    else:
        records = []

    observed_at = datetime(year, 1, 1)
    signals: list[MarketSignal] = []
    for record in records:
        geo_name = _extract_geo_name(record) or entity_id
        for metric, unit, value in _iter_fmr_values(record):
            signals.append(
                MarketSignal(
                    source="hud_fmr",
                    geo_level=geo_level,
                    geo_id=entity_id,
                    geo_name=geo_name,
                    observed_at=observed_at,
                    metric=metric,
                    value=value,
                    unit=unit,
                    raw_payload=record,
                )
            )

    return signals


__all__ = ["fetch_hud_fmr", "HUD_FMR_BASE_URL", "HUD_FMR_FIELDS"]
