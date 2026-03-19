from dataclasses import dataclass


def _norm_team(value: str) -> str:
    return " ".join(str(value or "").strip().lower().split())


@dataclass(frozen=True)
class SportsMarketSnapshot:
    event_id: str
    event_date_utc: str
    home_team: str
    away_team: str
    market_scope: str
    resolution_rule: str


@dataclass(frozen=True)
class SportsMatchResult:
    ok: bool
    reason_code: str
    detail: str


def validate_sports_match(kalshi: SportsMarketSnapshot, polymarket: SportsMarketSnapshot) -> SportsMatchResult:
    if kalshi.event_id and polymarket.event_id and kalshi.event_id != polymarket.event_id:
        return SportsMatchResult(False, "event_id_mismatch", "event_id differs between venues")

    if kalshi.event_date_utc != polymarket.event_date_utc:
        return SportsMatchResult(False, "event_date_mismatch", "event date differs between venues")

    k_home = _norm_team(kalshi.home_team)
    k_away = _norm_team(kalshi.away_team)
    p_home = _norm_team(polymarket.home_team)
    p_away = _norm_team(polymarket.away_team)
    if (k_home, k_away) != (p_home, p_away):
        return SportsMatchResult(False, "team_mismatch", "home/away teams differ between venues")

    if str(kalshi.market_scope).strip().lower() != str(polymarket.market_scope).strip().lower():
        return SportsMatchResult(False, "market_scope_mismatch", "market scope differs between venues")

    if str(kalshi.resolution_rule).strip().lower() != str(polymarket.resolution_rule).strip().lower():
        return SportsMatchResult(False, "resolution_rule_mismatch", "resolution rule differs between venues")

    return SportsMatchResult(True, "matched", "sports markets are compatible")
