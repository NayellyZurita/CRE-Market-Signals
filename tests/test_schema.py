from datetime import datetime, UTC

import pytest

from pipelines.model import MarketSignal


def test_market_signal_serialization_roundtrip():
    payload = {
        "source": "hud_fmr",
        "geo_level": "county",
        "geo_id": "49-035",
        "geo_name": "Salt Lake County, UT",
        "observed_at": datetime(2025, 1, 1),
        "metric": "fmr_2br",
        "value": 1350,
        "unit": "USD",
        "raw_payload": {"FMR_2": 1350},
    }

    signal = MarketSignal(**payload)

    assert signal.value == pytest.approx(1350.0)
    assert signal.observed_at.isoformat() == "2025-01-01T00:00:00"

    serialized = signal.model_dump()
    assert serialized["metric"] == "fmr_2br"
    assert isinstance(serialized["raw_payload"], dict)


def test_market_signal_requires_numeric_value():
    with pytest.raises(ValueError):
        MarketSignal(
            source="fred",
            geo_level="county",
            geo_id="49-035",
            geo_name="Salt Lake County, UT",
            observed_at=datetime.now(UTC),
            metric="unemp_rate",
            value="not-a-number",
            unit="%",
        )
