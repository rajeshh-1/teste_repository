from datetime import datetime, timezone

from bot.crypto_updown.matching import CryptoMarketSnapshot, validate_crypto_match
from bot.sports.matching import SportsMarketSnapshot, validate_sports_match


def test_crypto_match_accepts_same_window_and_resolution():
    ts = datetime(2026, 3, 19, 12, 0, tzinfo=timezone.utc)
    kalshi = CryptoMarketSnapshot(
        market_key="BTC15M_2026-03-19T12:15:00Z",
        market_close_utc="2026-03-19T12:15:00Z",
        resolution_rule="btc_above_floor_at_close",
        timestamp_utc=ts,
    )
    poly = CryptoMarketSnapshot(
        market_key="BTC15M_2026-03-19T12:15:00Z",
        market_close_utc="2026-03-19T12:15:00Z",
        resolution_rule="btc_above_floor_at_close",
        timestamp_utc=ts,
    )
    decision = validate_crypto_match(kalshi, poly, tolerance_sec=1.0)
    assert decision.ok is True
    assert decision.reason_code == "matched"


def test_crypto_match_rejects_market_key_mismatch():
    kalshi = CryptoMarketSnapshot(
        market_key="BTC15M_2026-03-19T12:15:00Z",
        market_close_utc="2026-03-19T12:15:00Z",
        resolution_rule="btc_above_floor_at_close",
    )
    poly = CryptoMarketSnapshot(
        market_key="BTC15M_2026-03-19T12:30:00Z",
        market_close_utc="2026-03-19T12:30:00Z",
        resolution_rule="btc_above_floor_at_close",
    )
    decision = validate_crypto_match(kalshi, poly)
    assert decision.ok is False
    assert decision.reason_code == "market_key_mismatch"


def test_sports_match_accepts_same_event():
    kalshi = SportsMarketSnapshot(
        event_id="NBA-20260319-LAL-BOS",
        event_date_utc="2026-03-19",
        home_team="Lakers",
        away_team="Celtics",
        market_scope="moneyline",
        resolution_rule="winner_final_score",
    )
    poly = SportsMarketSnapshot(
        event_id="NBA-20260319-LAL-BOS",
        event_date_utc="2026-03-19",
        home_team="Lakers",
        away_team="Celtics",
        market_scope="moneyline",
        resolution_rule="winner_final_score",
    )
    decision = validate_sports_match(kalshi, poly)
    assert decision.ok is True
    assert decision.reason_code == "matched"


def test_sports_match_rejects_scope_mismatch():
    kalshi = SportsMarketSnapshot(
        event_id="NBA-20260319-LAL-BOS",
        event_date_utc="2026-03-19",
        home_team="Lakers",
        away_team="Celtics",
        market_scope="spread",
        resolution_rule="winner_final_score",
    )
    poly = SportsMarketSnapshot(
        event_id="NBA-20260319-LAL-BOS",
        event_date_utc="2026-03-19",
        home_team="Lakers",
        away_team="Celtics",
        market_scope="moneyline",
        resolution_rule="winner_final_score",
    )
    decision = validate_sports_match(kalshi, poly)
    assert decision.ok is False
    assert decision.reason_code == "market_scope_mismatch"
