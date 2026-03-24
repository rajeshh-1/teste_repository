from datetime import date

import pytest

from scripts.fetch_sol_today_history import (
    MarketSpec,
    _market_key,
    _market_specs,
    _parse_bool,
    _parse_date,
)


def test_parse_bool_values():
    assert _parse_bool("true") is True
    assert _parse_bool("1") is True
    assert _parse_bool("false") is False
    assert _parse_bool("0") is False
    with pytest.raises(ValueError):
        _parse_bool("maybe")


def test_parse_date():
    d = _parse_date("2026-03-24")
    assert d == date(2026, 3, 24)


def test_market_specs_and_market_key():
    specs = _market_specs("both")
    names = {s.market for s in specs}
    assert names == {"SOL5M", "SOL15M"}

    spec = MarketSpec(market="SOL5M", coin="sol", timeframe="5m", bucket_min=5, out_folder="sol5m")
    key = _market_key(spec, "2026-03-24T12:00:00Z")
    assert key == "SOL5M_2026-03-24T12:00:00Z"
