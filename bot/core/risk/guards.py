import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from bot.core.reason_codes import ACCEPTED, CIRCUIT_BREAKER_TRIGGERED, KILL_SWITCH_ACTIVE


@dataclass(frozen=True)
class RiskLimits:
    max_losses_streak: int = 3
    max_daily_drawdown_pct: float = 20.0
    max_open_positions: int = 1
    kill_switch_path: str = ""


@dataclass(frozen=True)
class GuardDecision:
    ok: bool
    reason_code: str
    detail: str


class CircuitBreaker:
    def __init__(
        self,
        limits: RiskLimits,
        *,
        day_start_equity: float,
        now_fn: Callable[[], datetime] | None = None,
    ) -> None:
        self.limits = limits
        self._now_fn = now_fn or (lambda: datetime.now(timezone.utc))
        self.losses_streak = 0
        self.day_start_equity = max(0.0, float(day_start_equity))
        self.current_equity = self.day_start_equity
        self._day = self._now_fn().date()

    def _roll_day_if_needed(self) -> None:
        now_day = self._now_fn().date()
        if now_day == self._day:
            return
        self._day = now_day
        self.losses_streak = 0
        self.day_start_equity = self.current_equity

    def _drawdown_pct(self, current_equity: float) -> float:
        base = max(1e-12, self.day_start_equity)
        dd = max(0.0, self.day_start_equity - current_equity)
        return (dd / base) * 100.0

    def _kill_switch_active(self) -> bool:
        env_value = str(os.getenv("ARB_KILL_SWITCH", "")).strip().lower()
        if env_value in {"1", "true", "yes", "on"}:
            return True
        path = str(self.limits.kill_switch_path or "").strip()
        if not path:
            return False
        return Path(path).exists()

    def evaluate_entry(self, *, current_equity: float, open_positions: int) -> GuardDecision:
        self._roll_day_if_needed()
        equity = max(0.0, float(current_equity))
        self.current_equity = equity
        if self._kill_switch_active():
            return GuardDecision(False, KILL_SWITCH_ACTIVE, "manual kill switch is active")

        if int(open_positions) >= int(self.limits.max_open_positions):
            return GuardDecision(
                False,
                CIRCUIT_BREAKER_TRIGGERED,
                f"open_positions={int(open_positions)} >= max_open_positions={int(self.limits.max_open_positions)}",
            )
        if self.losses_streak >= int(self.limits.max_losses_streak):
            return GuardDecision(
                False,
                CIRCUIT_BREAKER_TRIGGERED,
                f"losses_streak={self.losses_streak} >= max_losses_streak={int(self.limits.max_losses_streak)}",
            )
        dd_pct = self._drawdown_pct(equity)
        if dd_pct >= float(self.limits.max_daily_drawdown_pct):
            return GuardDecision(
                False,
                CIRCUIT_BREAKER_TRIGGERED,
                f"daily_drawdown_pct={dd_pct:.4f} >= max_daily_drawdown_pct={float(self.limits.max_daily_drawdown_pct):.4f}",
            )
        return GuardDecision(True, ACCEPTED, "risk guard passed")

    def record_trade_result(self, *, realized_pnl: float, current_equity: float) -> None:
        self._roll_day_if_needed()
        self.current_equity = max(0.0, float(current_equity))
        pnl = float(realized_pnl)
        if pnl < 0:
            self.losses_streak += 1
        elif pnl > 0:
            self.losses_streak = 0

    def snapshot(self) -> dict:
        return {
            "losses_streak": int(self.losses_streak),
            "day_start_equity": float(self.day_start_equity),
            "current_equity": float(self.current_equity),
            "drawdown_pct": float(self._drawdown_pct(self.current_equity)),
            "kill_switch_active": bool(self._kill_switch_active()),
            "max_losses_streak": int(self.limits.max_losses_streak),
            "max_daily_drawdown_pct": float(self.limits.max_daily_drawdown_pct),
            "max_open_positions": int(self.limits.max_open_positions),
            "kill_switch_path": str(self.limits.kill_switch_path or ""),
        }
