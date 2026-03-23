from __future__ import annotations

import random
from dataclasses import dataclass

from .policy_grid import PolicyConfig


ACCEPTED = "accepted"
BELOW_MIN_EDGE = "below_min_edge"
LEG_TIMEOUT = "leg_timeout"
PARTIAL_FILL = "partial_fill"
HEDGE_FAILED = "hedge_failed"
UNWIND_EXECUTED = "unwind_executed"

VALID_REASON_CODES = {
    ACCEPTED,
    BELOW_MIN_EDGE,
    LEG_TIMEOUT,
    PARTIAL_FILL,
    HEDGE_FAILED,
    UNWIND_EXECUTED,
}


@dataclass(frozen=True)
class SyntheticEvent:
    market_key: str
    edge_liq_pct: float
    seconds_to_close: int
    leg2_latency_ms: int
    partial_fill_prob: float = 0.0
    timeout_prob: float = 0.0
    hedge_fail_prob: float = 0.0
    unwind_loss_bps: float = 0.0

    def __post_init__(self) -> None:
        if not str(self.market_key).strip():
            raise ValueError("market_key must be non-empty")
        if int(self.seconds_to_close) < 0:
            raise ValueError("seconds_to_close must be >= 0")
        if int(self.leg2_latency_ms) < 0:
            raise ValueError("leg2_latency_ms must be >= 0")
        if float(self.edge_liq_pct) < 0:
            raise ValueError("edge_liq_pct must be >= 0")
        if float(self.unwind_loss_bps) < 0:
            raise ValueError("unwind_loss_bps must be >= 0")
        for field_name, value in {
            "partial_fill_prob": self.partial_fill_prob,
            "timeout_prob": self.timeout_prob,
            "hedge_fail_prob": self.hedge_fail_prob,
        }.items():
            if not (0.0 <= float(value) <= 1.0):
                raise ValueError(f"{field_name} must be between 0 and 1")


@dataclass(frozen=True)
class Decision:
    policy_id: str
    event_index: int
    market_key: str
    accepted: bool
    reason_code: str
    detail: str

    def __post_init__(self) -> None:
        if self.reason_code not in VALID_REASON_CODES:
            raise ValueError(f"invalid reason_code={self.reason_code}")


@dataclass
class PolicySimulationResult:
    policy: PolicyConfig
    decisions: list[Decision]

    @property
    def accepted_count(self) -> int:
        return sum(1 for d in self.decisions if d.reason_code == ACCEPTED)


def _evaluate_event(policy: PolicyConfig, event: SyntheticEvent, event_index: int, rng: random.Random) -> Decision:
    if event.seconds_to_close < int(policy.entry_cutoff_sec):
        return Decision(
            policy_id=policy.policy_id,
            event_index=event_index,
            market_key=event.market_key,
            accepted=False,
            reason_code=BELOW_MIN_EDGE,
            detail=f"entry_cutoff_sec violated: {event.seconds_to_close} < {policy.entry_cutoff_sec}",
        )

    if float(event.edge_liq_pct) < float(policy.min_edge_liq_pct):
        return Decision(
            policy_id=policy.policy_id,
            event_index=event_index,
            market_key=event.market_key,
            accepted=False,
            reason_code=BELOW_MIN_EDGE,
            detail=f"edge_liq_pct {event.edge_liq_pct:.6f} < min_edge_liq_pct {policy.min_edge_liq_pct:.6f}",
        )

    if int(event.leg2_latency_ms) > int(policy.leg2_timeout_ms) or rng.random() < float(event.timeout_prob):
        return Decision(
            policy_id=policy.policy_id,
            event_index=event_index,
            market_key=event.market_key,
            accepted=False,
            reason_code=LEG_TIMEOUT,
            detail=f"leg2 latency/timeout failure (latency={event.leg2_latency_ms}ms, timeout_prob={event.timeout_prob:.4f})",
        )

    if rng.random() < float(event.partial_fill_prob):
        if float(event.unwind_loss_bps) > float(policy.max_unwind_loss_bps):
            return Decision(
                policy_id=policy.policy_id,
                event_index=event_index,
                market_key=event.market_key,
                accepted=False,
                reason_code=PARTIAL_FILL,
                detail=(
                    f"partial fill and unwind blocked: unwind_loss_bps {event.unwind_loss_bps:.4f} > "
                    f"max_unwind_loss_bps {policy.max_unwind_loss_bps:.4f}"
                ),
            )
        if rng.random() < float(event.hedge_fail_prob):
            return Decision(
                policy_id=policy.policy_id,
                event_index=event_index,
                market_key=event.market_key,
                accepted=False,
                reason_code=HEDGE_FAILED,
                detail=f"partial fill with hedge failure (hedge_fail_prob={event.hedge_fail_prob:.4f})",
            )
        return Decision(
            policy_id=policy.policy_id,
            event_index=event_index,
            market_key=event.market_key,
            accepted=False,
            reason_code=UNWIND_EXECUTED,
            detail=f"partial fill handled by unwind (loss_bps={event.unwind_loss_bps:.4f})",
        )

    return Decision(
        policy_id=policy.policy_id,
        event_index=event_index,
        market_key=event.market_key,
        accepted=True,
        reason_code=ACCEPTED,
        detail="trade accepted",
    )


def run_single_pass_multi_policy(
    *,
    events: list[SyntheticEvent],
    policies: list[PolicyConfig],
    seed: int,
) -> dict[str, PolicySimulationResult]:
    # One RNG per policy keeps deterministic streams independent of list ordering changes.
    rng_by_policy: dict[str, random.Random] = {
        p.policy_id: random.Random(int(seed) + idx) for idx, p in enumerate(policies)
    }
    results = {p.policy_id: PolicySimulationResult(policy=p, decisions=[]) for p in policies}
    accepted_markets: dict[str, set[str]] = {p.policy_id: set() for p in policies}

    for event_index, event in enumerate(events):
        for policy in policies:
            pid = policy.policy_id
            if event.market_key in accepted_markets[pid]:
                continue
            decision = _evaluate_event(policy=policy, event=event, event_index=event_index, rng=rng_by_policy[pid])
            results[pid].decisions.append(decision)
            if decision.reason_code == ACCEPTED:
                accepted_markets[pid].add(event.market_key)
    return results
