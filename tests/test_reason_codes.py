from bot.core.reason_codes import CORE_REASON_CODES


def test_required_reason_codes_are_present():
    required = {
        "accepted",
        "below_min_edge",
        "insufficient_liquidity",
        "partial_fill",
        "hedge_failed",
        "circuit_breaker_triggered",
        "kill_switch_active",
    }
    assert required.issubset(CORE_REASON_CODES)
