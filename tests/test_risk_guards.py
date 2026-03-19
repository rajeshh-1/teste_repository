from datetime import datetime, timezone

from bot.core.reason_codes import CIRCUIT_BREAKER_TRIGGERED, KILL_SWITCH_ACTIVE
from bot.core.risk.guards import CircuitBreaker, RiskLimits


class _Clock:
    def __init__(self, now):
        self.now = now

    def __call__(self):
        return self.now


def test_breaker_triggers_by_losses_streak():
    clock = _Clock(datetime(2026, 3, 19, 12, 0, tzinfo=timezone.utc))
    guard = CircuitBreaker(RiskLimits(max_losses_streak=2, max_daily_drawdown_pct=50.0, max_open_positions=1), day_start_equity=100.0, now_fn=clock)
    guard.record_trade_result(realized_pnl=-1.0, current_equity=99.0)
    guard.record_trade_result(realized_pnl=-1.0, current_equity=98.0)
    decision = guard.evaluate_entry(current_equity=98.0, open_positions=0)
    assert decision.ok is False
    assert decision.reason_code == CIRCUIT_BREAKER_TRIGGERED
    assert "losses_streak" in decision.detail


def test_breaker_triggers_by_daily_drawdown():
    clock = _Clock(datetime(2026, 3, 19, 12, 0, tzinfo=timezone.utc))
    guard = CircuitBreaker(RiskLimits(max_losses_streak=10, max_daily_drawdown_pct=5.0, max_open_positions=1), day_start_equity=100.0, now_fn=clock)
    decision = guard.evaluate_entry(current_equity=94.0, open_positions=0)
    assert decision.ok is False
    assert decision.reason_code == CIRCUIT_BREAKER_TRIGGERED
    assert "daily_drawdown_pct" in decision.detail


def test_kill_switch_blocks_new_entry(tmp_path):
    kill_flag = tmp_path / "kill.flag"
    kill_flag.write_text("1", encoding="utf-8")
    clock = _Clock(datetime(2026, 3, 19, 12, 0, tzinfo=timezone.utc))
    guard = CircuitBreaker(
        RiskLimits(
            max_losses_streak=10,
            max_daily_drawdown_pct=90.0,
            max_open_positions=1,
            kill_switch_path=str(kill_flag),
        ),
        day_start_equity=100.0,
        now_fn=clock,
    )
    decision = guard.evaluate_entry(current_equity=100.0, open_positions=0)
    assert decision.ok is False
    assert decision.reason_code == KILL_SWITCH_ACTIVE
