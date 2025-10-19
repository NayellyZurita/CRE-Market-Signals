"""HUD Fair Market Rent ingestor.

Turns HUD API responses into normalized ``MarketSignal`` records that downstream
pipelines can persist or serve.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, UTC
from typing import Any, Iterable, Mapping

from httpx import HTTPStatusError

from pipelines.common import fetch_json
from pipelines.model import MarketSignal

HUD_FMR_BASE_URL = "https://www.huduser.gov/hudapi/public/fmr/data"

# Canonical metric -> (HUD field name in flat shape, unit)
HUD_FMR_FIELDS: Mapping[str, tuple[str, str]] = {
    "fmr_0br": ("fmr_0br", "USD"),
    "fmr_1br": ("fmr_1br", "USD"),
    "fmr_2br": ("fmr_2br", "USD"),
    "fmr_3br": ("fmr_3br", "USD"),
    "fmr_4br": ("fmr_4br", "USD"),
}

# Mapping of nested HUD basicdata labels to canonical metrics
HUD_BASICDATA_FIELDS: Mapping[str, tuple[str, str]] = {
    "Efficiency": ("fmr_0br", "USD"),
    "One-Bedroom": ("fmr_1br", "USD"),
    "Two-Bedroom": ("fmr_2br", "USD"),
    "Three-Bedroom": ("fmr_3br", "USD"),
    "Four-Bedroom": ("fmr_4br", "USD"),
}

logger = logging.getLogger(__name__)


def _resolve_token(token: str | None) -> str | None:
    resolved = token or os.getenv("HUD_TOKEN")
    if not resolved:
        logger.warning(
            "HUD API token missing. Skipping HUD FMR fetch. Set HUD_TOKEN or pass token explicitly."
        )
    return resolved


def _to_hud_entityid(geo: str) -> str:
    """
    Accepts '49-035', '49035', 'county:4903599999', or '4903599999'
    and returns the 10-digit HUD entity id (county FIPS padded with 9s).
    """
    s = re.sub(r"\D", "", geo or "")
    if len(s) == 5:
        return (s + "9999").ljust(10, "9")
    if len(s) == 10 and s.endswith("9999"):
        return s
    raise ValueError(f"Unrecognized HUD geo format: {geo!r}")


def _iter_fmr_values(record: Mapping[str, Any]) -> Iterable[tuple[str, str, float]]:
    """
    Yields (metric, unit, value) triples from either the nested 'basicdata' shape
    or the older flat 'fmr_*' shape.
    """
    # 1) Nested basicdata (current API)
    basic = record.get("basicdata")
    if isinstance(basic, Mapping):
        for hud_key, (metric, unit) in HUD_BASICDATA_FIELDS.items():
            raw_value = basic.get(hud_key)
            if raw_value in (None, "", "NA", "N/A"):
                continue
            try:
                yield metric, unit, float(raw_value)
            except (TypeError, ValueError):
                continue
        return

    # 2) Flat shape fallback: keys like 'fmr_2br' at top level
    for metric, (field_name, unit) in HUD_FMR_FIELDS.items():
        raw_value = record.get(field_name)
        if raw_value in (None, "", "NA", "N/A"):
            continue
        try:
            yield metric, unit, float(raw_value)
        except (TypeError, ValueError):
            continue


def _extract_geo_name(record: Mapping[str, Any]) -> str | None:
    # Prefer the richer labels HUD returns
    candidates = []
    for key in ("area_name", "metro_name", "county_name", "cbsa_name", "county", "name"):
        val = record.get(key)
        if isinstance(val, str) and val.strip():
            candidates.append(val.strip())
    state = record.get("state")
    if isinstance(state, str) and state.strip():
        candidates.append(state.strip())
    if candidates:
        # maintain order, remove duplicates
        return ", ".join(dict.fromkeys(candidates))
    return None


async def fetch_hud_fmr(
    entity_id: str,
    *,
    geo_level: str,
    year: int,
    token: str | None = None,
    params: Mapping[str, Any] | None = None,
) -> list[MarketSignal]:
    """Fetch HUD Fair Market Rent data for a geography and normalize it."""
    resolved_token = _resolve_token(token)
    if not resolved_token:
        return []

    # Normalize whatever was passed (e.g., '49-035' -> '4903599999')
    try:
        hud_entity = _to_hud_entityid(entity_id)
    except ValueError as e:
        logger.warning("HUD FMR: %s. Skipping.", e)
        return []

    request_params = {"year": year}
    if params:
        request_params.update(params)

    try:
        payload = await fetch_json(
            f"{HUD_FMR_BASE_URL}/{hud_entity}",
            headers={"Authorization": f"Bearer {resolved_token}", "Accept": "application/json"},
            params=request_params,
        )
    except HTTPStatusError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        logger.warning(
            "HUD FMR request failed for %s (normalized from %s, %s) year=%s status=%s. Skipping.",
            hud_entity,
            entity_id,
            geo_level,
            year,
            status,
        )
        return []

    # Normalize payload into a list of mapping records
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

    # Observed timestamp: prefer response year if present in basicdata, else parameter year
    resp_year = None
    if records and isinstance(records[0].get("basicdata"), Mapping):
        try:
            resp_year = int(records[0]["basicdata"].get("year") or year)
        except (TypeError, ValueError):
            resp_year = year
    else:
        resp_year = year

    observed_at = datetime(resp_year, 1, 1, tzinfo=UTC)

    signals: list[MarketSignal] = []
    for record in records:
        geo_name = _extract_geo_name(record) or hud_entity
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
