from bot.crypto_updown.runtime.policy_grid import PolicyConfig, generate_policy_grid
from bot.crypto_updown.runtime.single_pass_simulator import (
    ACCEPTED,
    BELOW_MIN_EDGE,
    HEDGE_FAILED,
    LEG_TIMEOUT,
    PARTIAL_FILL,
    UNWIND_EXECUTED,
    SyntheticEvent,
    run_single_pass_multi_policy,
)


def _build_events() -> list[SyntheticEvent]:
    return [
        SyntheticEvent(
            market_key="SOL5M_001",
            edge_liq_pct=3.0,
            seconds_to_close=90,
            leg2_latency_ms=100,
            partial_fill_prob=0.0,
            timeout_prob=0.0,
            hedge_fail_prob=0.0,
            unwind_loss_bps=2.0,
        ),
        SyntheticEvent(
            market_key="SOL5M_002",
            edge_liq_pct=8.0,
            seconds_to_close=120,
            leg2_latency_ms=500,
            partial_fill_prob=0.0,
            timeout_prob=0.0,
            hedge_fail_prob=0.0,
            unwind_loss_bps=2.0,
        ),
        SyntheticEvent(
            market_key="SOL5M_003",
            edge_liq_pct=9.0,
            seconds_to_close=120,
            leg2_latency_ms=100,
            partial_fill_prob=1.0,
            timeout_prob=0.0,
            hedge_fail_prob=0.0,
            unwind_loss_bps=100.0,
        ),
        SyntheticEvent(
            market_key="SOL5M_004",
            edge_liq_pct=9.0,
            seconds_to_close=120,
            leg2_latency_ms=100,
            partial_fill_prob=1.0,
            timeout_prob=0.0,
            hedge_fail_prob=0.0,
            unwind_loss_bps=5.0,
        ),
        SyntheticEvent(
            market_key="SOL5M_005",
            edge_liq_pct=9.0,
            seconds_to_close=120,
            leg2_latency_ms=100,
            partial_fill_prob=1.0,
            timeout_prob=0.0,
            hedge_fail_prob=1.0,
            unwind_loss_bps=5.0,
        ),
        SyntheticEvent(
            market_key="SOL5M_006",
            edge_liq_pct=10.0,
            seconds_to_close=120,
            leg2_latency_ms=100,
            partial_fill_prob=0.0,
            timeout_prob=0.0,
            hedge_fail_prob=0.0,
            unwind_loss_bps=1.0,
        ),
    ]


def test_generate_policy_grid_and_unique_ids():
    grid = generate_policy_grid(
        leg2_timeout_ms_values=[200, 400],
        min_edge_liq_pct_values=[5.0, 7.0],
        max_unwind_loss_bps_values=[5.0],
        entry_cutoff_sec_values=[30],
    )
    assert len(grid) == 4
    assert all(p.max_trades_per_market == 1 for p in grid)
    ids = {p.policy_id for p in grid}
    assert len(ids) == len(grid)


def test_policy_enforces_max_trades_per_market_fixed_one():
    try:
        PolicyConfig(
            leg2_timeout_ms=100,
            min_edge_liq_pct=5.0,
            max_unwind_loss_bps=5.0,
            entry_cutoff_sec=10,
            max_trades_per_market=2,
        )
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "max_trades_per_market" in str(exc)


def test_single_pass_reason_codes_covered():
    policy = PolicyConfig(
        leg2_timeout_ms=200,
        min_edge_liq_pct=5.0,
        max_unwind_loss_bps=10.0,
        entry_cutoff_sec=30,
    )
    events = _build_events()
    out = run_single_pass_multi_policy(events=events, policies=[policy], seed=7)
    decisions = out[policy.policy_id].decisions
    reasons = {d.reason_code for d in decisions}
    assert BELOW_MIN_EDGE in reasons
    assert LEG_TIMEOUT in reasons
    assert PARTIAL_FILL in reasons
    assert UNWIND_EXECUTED in reasons
    assert HEDGE_FAILED in reasons
    assert ACCEPTED in reasons


def test_single_pass_is_deterministic_by_seed():
    policy = PolicyConfig(
        leg2_timeout_ms=250,
        min_edge_liq_pct=5.0,
        max_unwind_loss_bps=12.0,
        entry_cutoff_sec=20,
    )
    events = [
        SyntheticEvent(
            market_key=f"SOL5M_{idx:03d}",
            edge_liq_pct=6.0 + (idx % 3),
            seconds_to_close=100,
            leg2_latency_ms=200,
            partial_fill_prob=0.35,
            timeout_prob=0.15,
            hedge_fail_prob=0.20,
            unwind_loss_bps=8.0,
        )
        for idx in range(30)
    ]
    run_a = run_single_pass_multi_policy(events=events, policies=[policy], seed=42)
    run_b = run_single_pass_multi_policy(events=events, policies=[policy], seed=42)
    run_c = run_single_pass_multi_policy(events=events, policies=[policy], seed=99)

    seq_a = [(d.market_key, d.reason_code, d.accepted) for d in run_a[policy.policy_id].decisions]
    seq_b = [(d.market_key, d.reason_code, d.accepted) for d in run_b[policy.policy_id].decisions]
    seq_c = [(d.market_key, d.reason_code, d.accepted) for d in run_c[policy.policy_id].decisions]

    assert seq_a == seq_b
    assert seq_a != seq_c
