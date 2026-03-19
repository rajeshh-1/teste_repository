from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class CryptoMarketSnapshot:
    market_key: str
    market_close_utc: str
    resolution_rule: str
    timestamp_utc: Optional[datetime] = None


@dataclass(frozen=True)
class CryptoMatchResult:
    ok: bool
    reason_code: str
    detail: str


def validate_crypto_match(
    kalshi: CryptoMarketSnapshot,
    polymarket: CryptoMarketSnapshot,
    *,
    tolerance_sec: float = 1.0,
) -> CryptoMatchResult:
    if not kalshi.market_key or not polymarket.market_key:
        return CryptoMatchResult(False, "missing_market_key", "market_key is required on both venues")
    if kalshi.market_key != polymarket.market_key:
        return CryptoMatchResult(False, "market_key_mismatch", "market_key differs between venues")
    if not kalshi.market_close_utc or not polymarket.market_close_utc:
        return CryptoMatchResult(False, "missing_market_close", "market_close_utc is required on both venues")
    if kalshi.market_close_utc != polymarket.market_close_utc:
        return CryptoMatchResult(False, "close_window_mismatch", "market close window differs between venues")
    if str(kalshi.resolution_rule).strip().lower() != str(polymarket.resolution_rule).strip().lower():
        return CryptoMatchResult(False, "resolution_rule_mismatch", "resolution rule differs between venues")

    if kalshi.timestamp_utc is not None and polymarket.timestamp_utc is not None:
        diff = abs((kalshi.timestamp_utc - polymarket.timestamp_utc).total_seconds())
        if diff > float(tolerance_sec):
            return CryptoMatchResult(False, "stale_timestamp_mismatch", f"timestamp drift={diff:.3f}s")

    return CryptoMatchResult(True, "matched", "crypto markets are compatible")
