"""Canonical data model for market signals ingested from external APIs."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


class MarketSignal(BaseModel):
    """Normalized representation of a single market metric observation."""

    source: str = Field(
        ..., description="Upstream API or dataset identifier (e.g. 'hud_fmr', 'fred')."
    )
    geo_level: str = Field(
        ..., description="Granularity of the geography (e.g. 'county', 'cbsa', 'city')."
    )
    geo_id: str = Field(
        ..., description="Stable unique identifier for the geography (FIPS, CBSA code, etc.)."
    )
    geo_name: str = Field(
        ..., description="Human-readable geography name suitable for display."
    )
    observed_at: datetime = Field(
        ...,
        description="Timestamp representing when the metric was observed or is effective.",
    )
    metric: str = Field(
        ..., description="Canonical metric key (e.g. 'fmr_2br', 'median_hh_income')."
    )
    value: float = Field(
        ..., description="Numeric value of the observed metric normalized to float."
    )
    unit: str = Field(
        ..., description="Measurement unit associated with the value (e.g. 'USD', '%')."
    )
    raw_payload: Optional[Any] = Field(
        default=None,
        description="Raw upstream payload segment retained for traceability and debugging.",
    )

    class Config:
        anystr_strip_whitespace = True
        allow_mutation = False
        json_encoders = {datetime: lambda dt: dt.isoformat()}
