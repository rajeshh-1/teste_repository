from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from itertools import product


@dataclass(frozen=True)
class PolicyConfig:
    leg2_timeout_ms: int
    min_edge_liq_pct: float
    max_unwind_loss_bps: float
    entry_cutoff_sec: int
    max_trades_per_market: int = 1

    def __post_init__(self) -> None:
        if int(self.leg2_timeout_ms) < 0:
            raise ValueError("leg2_timeout_ms must be >= 0")
        if float(self.min_edge_liq_pct) < 0:
            raise ValueError("min_edge_liq_pct must be >= 0")
        if float(self.max_unwind_loss_bps) < 0:
            raise ValueError("max_unwind_loss_bps must be >= 0")
        if int(self.entry_cutoff_sec) < 0:
            raise ValueError("entry_cutoff_sec must be >= 0")
        if int(self.max_trades_per_market) != 1:
            raise ValueError("max_trades_per_market is fixed to 1 for single-pass mode")

    def as_dict(self) -> dict[str, float | int]:
        return {
            "leg2_timeout_ms": int(self.leg2_timeout_ms),
            "min_edge_liq_pct": float(self.min_edge_liq_pct),
            "max_unwind_loss_bps": float(self.max_unwind_loss_bps),
            "entry_cutoff_sec": int(self.entry_cutoff_sec),
            "max_trades_per_market": int(self.max_trades_per_market),
        }

    @property
    def policy_id(self) -> str:
        payload = json.dumps(self.as_dict(), sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]
        return f"policy_{digest}"


def generate_policy_grid(
    *,
    leg2_timeout_ms_values: list[int],
    min_edge_liq_pct_values: list[float],
    max_unwind_loss_bps_values: list[float],
    entry_cutoff_sec_values: list[int],
) -> list[PolicyConfig]:
    grid: list[PolicyConfig] = []
    for leg2_timeout_ms, min_edge_liq_pct, max_unwind_loss_bps, entry_cutoff_sec in product(
        leg2_timeout_ms_values,
        min_edge_liq_pct_values,
        max_unwind_loss_bps_values,
        entry_cutoff_sec_values,
    ):
        grid.append(
            PolicyConfig(
                leg2_timeout_ms=int(leg2_timeout_ms),
                min_edge_liq_pct=float(min_edge_liq_pct),
                max_unwind_loss_bps=float(max_unwind_loss_bps),
                entry_cutoff_sec=int(entry_cutoff_sec),
                max_trades_per_market=1,
            )
        )
    return grid
