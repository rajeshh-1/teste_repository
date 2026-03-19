from bot.core.reason_codes import HEDGE_FAILED, KILL_SWITCH_ACTIVE, LEG_TIMEOUT, PARTIAL_FILL
from bot.core.risk.guards import CircuitBreaker, RiskLimits
from bot.crypto_updown.runtime.live_runtime import (
    CryptoExecutionRuntime,
    LegExecutionResult,
    LegOrderRequest,
)


def _allowing_guard():
    return CircuitBreaker(
        RiskLimits(max_losses_streak=10, max_daily_drawdown_pct=90.0, max_open_positions=1),
        day_start_equity=100.0,
    )


def _legs():
    leg_a = LegOrderRequest(
        leg_name="leg_a",
        venue="kalshi",
        side="yes",
        price=0.45,
        quantity=10.0,
        timeout_sec=1.0,
    )
    leg_b = LegOrderRequest(
        leg_name="leg_b",
        venue="polymarket",
        side="down",
        price=0.44,
        quantity=10.0,
        timeout_sec=1.0,
    )
    return leg_a, leg_b


def test_execution_partial_fill_calls_hedge_path():
    runtime = CryptoExecutionRuntime(risk_guard=_allowing_guard())
    leg_a, leg_b = _legs()
    calls = {"hedge": 0}

    def execute_leg(leg):
        if leg.leg_name == "leg_a":
            return LegExecutionResult(status="filled", filled_qty=10.0, reason_code="accepted", detail="ok")
        return LegExecutionResult(status=PARTIAL_FILL, filled_qty=7.0, reason_code=PARTIAL_FILL, detail="partial")

    def hedge(_a, _b):
        calls["hedge"] += 1
        return True

    decision = runtime.execute(
        trade_id="T001",
        market_key="BTC15M_2026-03-19T12:15:00Z",
        strategy="A_KALSHI_YES_PLUS_POLY_DOWN",
        current_equity=100.0,
        open_positions=0,
        edge_liquido_pct=6.0,
        liq_k=100.0,
        liq_p=100.0,
        pretrade_revalidate=lambda: (True, "accepted", "ok"),
        leg_a=leg_a,
        leg_b=leg_b,
        execute_leg=execute_leg,
        hedge_flatten=hedge,
    )
    assert decision.accepted is False
    assert decision.reason_code == PARTIAL_FILL
    assert decision.hedge_attempted is True
    assert decision.hedge_ok is True
    assert calls["hedge"] == 1


def test_execution_partial_fill_hedge_failure():
    runtime = CryptoExecutionRuntime(risk_guard=_allowing_guard())
    leg_a, leg_b = _legs()

    def execute_leg(_leg):
        return LegExecutionResult(status=PARTIAL_FILL, filled_qty=5.0, reason_code=PARTIAL_FILL, detail="partial")

    decision = runtime.execute(
        trade_id="T002",
        market_key="BTC15M_2026-03-19T12:15:00Z",
        strategy="A_KALSHI_YES_PLUS_POLY_DOWN",
        current_equity=100.0,
        open_positions=0,
        edge_liquido_pct=6.0,
        liq_k=100.0,
        liq_p=100.0,
        pretrade_revalidate=lambda: (True, "accepted", "ok"),
        leg_a=leg_a,
        leg_b=leg_b,
        execute_leg=execute_leg,
        hedge_flatten=lambda _a, _b: False,
    )
    assert decision.accepted is False
    assert decision.reason_code == HEDGE_FAILED
    assert decision.hedge_attempted is True
    assert decision.hedge_ok is False


def test_execution_timeout_returns_leg_timeout():
    runtime = CryptoExecutionRuntime(risk_guard=_allowing_guard())
    leg_a, leg_b = _legs()

    def execute_leg(leg):
        if leg.leg_name == "leg_a":
            return LegExecutionResult(status=LEG_TIMEOUT, filled_qty=0.0, reason_code=LEG_TIMEOUT, detail="timeout")
        return LegExecutionResult(status="filled", filled_qty=10.0, reason_code="accepted", detail="ok")

    decision = runtime.execute(
        trade_id="T003",
        market_key="BTC15M_2026-03-19T12:15:00Z",
        strategy="A_KALSHI_YES_PLUS_POLY_DOWN",
        current_equity=100.0,
        open_positions=0,
        edge_liquido_pct=6.0,
        liq_k=100.0,
        liq_p=100.0,
        pretrade_revalidate=lambda: (True, "accepted", "ok"),
        leg_a=leg_a,
        leg_b=leg_b,
        execute_leg=execute_leg,
        hedge_flatten=lambda _a, _b: True,
    )
    assert decision.accepted is False
    assert decision.reason_code == LEG_TIMEOUT


def test_execution_blocked_by_kill_switch(tmp_path):
    kill_flag = tmp_path / "kill.flag"
    kill_flag.write_text("1", encoding="utf-8")
    guard = CircuitBreaker(
        RiskLimits(
            max_losses_streak=10,
            max_daily_drawdown_pct=90.0,
            max_open_positions=1,
            kill_switch_path=str(kill_flag),
        ),
        day_start_equity=100.0,
    )
    runtime = CryptoExecutionRuntime(risk_guard=guard)
    leg_a, leg_b = _legs()
    decision = runtime.execute(
        trade_id="T004",
        market_key="BTC15M_2026-03-19T12:15:00Z",
        strategy="A_KALSHI_YES_PLUS_POLY_DOWN",
        current_equity=100.0,
        open_positions=0,
        edge_liquido_pct=6.0,
        liq_k=100.0,
        liq_p=100.0,
        pretrade_revalidate=lambda: (True, "accepted", "ok"),
        leg_a=leg_a,
        leg_b=leg_b,
        execute_leg=lambda _leg: LegExecutionResult(status="filled", filled_qty=10.0, reason_code="accepted", detail="ok"),
        hedge_flatten=lambda _a, _b: True,
    )
    assert decision.accepted is False
    assert decision.reason_code == KILL_SWITCH_ACTIVE
