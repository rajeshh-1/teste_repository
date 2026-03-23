from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

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

GO_HEDGE_FAILED_MAX_RATE = 0.015
GO_MAX_DRAWDOWN_PCT = 12.0
GO_MAX_P99_LOSS = 0.40


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run SOL 5m single-pass replay over a multi-policy grid.")
    parser.add_argument("--market", required=True, help="Market label (example: SOL5M)")
    parser.add_argument("--input", required=True, help="Replay CSV input path")
    parser.add_argument("--grid-file", default="configs/sol5m_policy_grid.json")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out-dir", default="reports/sol5m_grid_single_pass")
    parser.add_argument("--search-mode", choices=["full", "successive-halving"], default="full")
    return parser.parse_args(argv)


def _parse_dt(value: str) -> datetime | None:
    txt = str(value or "").strip()
    if not txt:
        return None
    txt = txt.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(txt)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _read_float(row: dict[str, Any], keys: list[str], default: float | None = None) -> float | None:
    for key in keys:
        if key not in row:
            continue
        value = str(row.get(key, "")).strip()
        if not value:
            continue
        try:
            return float(value)
        except ValueError:
            continue
    return default


def _read_int(row: dict[str, Any], keys: list[str], default: int | None = None) -> int | None:
    for key in keys:
        if key not in row:
            continue
        value = str(row.get(key, "")).strip()
        if not value:
            continue
        try:
            return int(float(value))
        except ValueError:
            continue
    return default


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _derive_edge_pct(row: dict[str, Any]) -> float:
    edge = _read_float(row, ["edge_liq_pct", "edge_pct", "edge_liquido_pct"], default=None)
    if edge is not None:
        return max(0.0, float(edge))
    yes_ask = _read_float(row, ["yes_ask"], default=None)
    no_ask = _read_float(row, ["no_ask"], default=None)
    if yes_ask is not None and no_ask is not None:
        cost = max(1e-9, float(yes_ask) + float(no_ask))
        return max(0.0, ((1.0 - cost) / cost) * 100.0)
    return 0.0


def _derive_seconds_to_close(row: dict[str, Any]) -> int:
    sec = _read_int(row, ["seconds_to_close", "sec_to_close", "time_to_close_sec"], default=None)
    if sec is not None:
        return max(0, int(sec))
    now_dt = _parse_dt(str(row.get("timestamp_utc", "")).strip()) or _parse_dt(str(row.get("timestamp", "")).strip())
    close_dt = _parse_dt(str(row.get("market_close_utc", "")).strip()) or _parse_dt(str(row.get("close_time", "")).strip())
    if now_dt is not None and close_dt is not None:
        return max(0, int((close_dt - now_dt).total_seconds()))
    return 120


def _load_replay_events(input_path: Path, market: str) -> list[SyntheticEvent]:
    events: list[SyntheticEvent] = []
    with input_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for idx, row in enumerate(reader):
            market_key = str(row.get("market_key", "")).strip() or f"{market}_{idx:06d}"
            edge_liq_pct = _derive_edge_pct(row)
            seconds_to_close = _derive_seconds_to_close(row)
            leg2_latency_ms = _read_int(row, ["leg2_latency_ms", "latency_ms_leg2", "latency_ms"], default=300) or 300
            partial_fill_prob = _clamp01(
                _read_float(row, ["partial_fill_prob", "partial_prob", "partial_probability"], default=0.10) or 0.10
            )
            timeout_prob = _clamp01(
                _read_float(row, ["timeout_prob", "leg_timeout_prob", "timeout_probability"], default=0.03) or 0.03
            )
            hedge_fail_prob = _clamp01(
                _read_float(row, ["hedge_fail_prob", "hedge_failure_prob"], default=0.03) or 0.03
            )
            unwind_loss_bps = max(
                0.0,
                float(_read_float(row, ["unwind_loss_bps", "expected_unwind_loss_bps"], default=50.0) or 50.0),
            )
            events.append(
                SyntheticEvent(
                    market_key=market_key,
                    edge_liq_pct=edge_liq_pct,
                    seconds_to_close=seconds_to_close,
                    leg2_latency_ms=max(0, int(leg2_latency_ms)),
                    partial_fill_prob=partial_fill_prob,
                    timeout_prob=timeout_prob,
                    hedge_fail_prob=hedge_fail_prob,
                    unwind_loss_bps=unwind_loss_bps,
                )
            )
    return events


def _expand_values(raw: Any) -> list[int | float]:
    if isinstance(raw, list):
        return list(raw)
    if isinstance(raw, dict):
        start = int(raw["start"])
        stop = int(raw["stop"])
        step = int(raw["step"])
        if step <= 0:
            raise ValueError("range step must be > 0")
        if stop < start:
            raise ValueError("range stop must be >= start")
        return list(range(start, stop + 1, step))
    raise ValueError(f"invalid grid value: {raw!r}")


def _load_grid(grid_file: Path) -> list[PolicyConfig]:
    payload = json.loads(grid_file.read_text(encoding="utf-8"))
    grid = generate_policy_grid(
        leg2_timeout_ms_values=[int(v) for v in _expand_values(payload["leg2_timeout_ms"])],
        min_edge_liq_pct_values=[float(v) for v in _expand_values(payload["min_edge_liq_pct"])],
        max_unwind_loss_bps_values=[float(v) for v in _expand_values(payload["max_unwind_loss_bps"])],
        entry_cutoff_sec_values=[int(v) for v in _expand_values(payload["entry_cutoff_sec"])],
    )
    max_trades_per_market = int(payload.get("max_trades_per_market", 1))
    if max_trades_per_market != 1:
        raise ValueError("max_trades_per_market must be 1 for single-pass mode")
    return grid


def _safe_rate(part: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return float(part) / float(total)


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    q_clamped = max(0.0, min(1.0, float(q)))
    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * q_clamped
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return ordered[lo]
    weight_hi = pos - lo
    weight_lo = 1.0 - weight_hi
    return (ordered[lo] * weight_lo) + (ordered[hi] * weight_hi)


def _max_drawdown_pct(pnl_series: list[float], initial_equity: float = 100.0) -> float:
    equity = float(initial_equity)
    peak = float(initial_equity)
    max_dd = 0.0
    for pnl in pnl_series:
        equity += float(pnl)
        peak = max(peak, equity)
        dd = ((peak - equity) / max(1e-12, peak)) * 100.0
        max_dd = max(max_dd, dd)
    return max_dd


def _normalize(values: list[float]) -> list[float]:
    if not values:
        return []
    lo = min(values)
    hi = max(values)
    if abs(hi - lo) < 1e-12:
        return [1.0 for _ in values]
    return [(float(v) - lo) / (hi - lo) for v in values]


def _evaluate(events: list[SyntheticEvent], policies: list[PolicyConfig], seed: int) -> list[dict[str, Any]]:
    out = run_single_pass_multi_policy(events=events, policies=policies, seed=seed)
    rows: list[dict[str, Any]] = []
    for policy in policies:
        decisions = out[policy.policy_id].decisions
        trades_attempted = len(decisions)
        reason_counts = {
            ACCEPTED: 0,
            BELOW_MIN_EDGE: 0,
            LEG_TIMEOUT: 0,
            PARTIAL_FILL: 0,
            HEDGE_FAILED: 0,
            UNWIND_EXECUTED: 0,
        }
        pnl_series: list[float] = []
        losses: list[float] = []
        predicted_edges: list[float] = []
        captured_edges: list[float] = []
        for d in decisions:
            reason_counts[d.reason_code] = reason_counts.get(d.reason_code, 0) + 1
            pnl_series.append(float(d.pnl_usd))
            losses.append(max(0.0, -float(d.pnl_usd)))
            predicted_edges.append(float(d.predicted_edge_pct))
            captured_edges.append(float(d.captured_edge_pct))

        trades_accepted = reason_counts[ACCEPTED]
        fill_full_rate = _safe_rate(trades_accepted, trades_attempted)
        below_rate = _safe_rate(reason_counts[BELOW_MIN_EDGE], trades_attempted)
        timeout_rate = _safe_rate(reason_counts[LEG_TIMEOUT], trades_attempted)
        partial_rate = _safe_rate(reason_counts[PARTIAL_FILL], trades_attempted)
        hedge_failed_rate = _safe_rate(reason_counts[HEDGE_FAILED], trades_attempted)
        skip_rate = _safe_rate(trades_attempted - trades_accepted, trades_attempted)
        unwind_count = int(reason_counts[UNWIND_EXECUTED])

        avg_edge_predicted_pct = sum(predicted_edges) / len(predicted_edges) if predicted_edges else 0.0
        avg_edge_captured_pct = sum(captured_edges) / len(captured_edges) if captured_edges else 0.0
        edge_capture_ratio = (
            avg_edge_captured_pct / avg_edge_predicted_pct if abs(avg_edge_predicted_pct) > 1e-12 else 0.0
        )
        pnl_total = sum(pnl_series)
        pnl_per_trade = (pnl_total / trades_attempted) if trades_attempted else 0.0
        max_drawdown_pct = _max_drawdown_pct(pnl_series)
        p95_loss = _quantile(losses, 0.95)
        p99_loss = _quantile(losses, 0.99)

        go_no_go = (
            hedge_failed_rate <= GO_HEDGE_FAILED_MAX_RATE
            and pnl_per_trade > 0.0
            and max_drawdown_pct <= GO_MAX_DRAWDOWN_PCT
            and p99_loss <= GO_MAX_P99_LOSS
        )
        go_reasons: list[str] = []
        if hedge_failed_rate > GO_HEDGE_FAILED_MAX_RATE:
            go_reasons.append("hedge_failed_rate")
        if pnl_per_trade <= 0.0:
            go_reasons.append("pnl_per_trade")
        if max_drawdown_pct > GO_MAX_DRAWDOWN_PCT:
            go_reasons.append("max_drawdown_pct")
        if p99_loss > GO_MAX_P99_LOSS:
            go_reasons.append("p99_loss")

        rows.append(
            {
                "policy_id": policy.policy_id,
                "leg2_timeout_ms": int(policy.leg2_timeout_ms),
                "min_edge_liq_pct": float(policy.min_edge_liq_pct),
                "max_unwind_loss_bps": float(policy.max_unwind_loss_bps),
                "entry_cutoff_sec": int(policy.entry_cutoff_sec),
                "max_trades_per_market": int(policy.max_trades_per_market),
                "trades_attempted": trades_attempted,
                "trades_accepted": trades_accepted,
                "fill_full_rate": round(fill_full_rate, 6),
                "below_min_edge_rate": round(below_rate, 6),
                "leg_timeout_rate": round(timeout_rate, 6),
                "partial_fill_rate": round(partial_rate, 6),
                "hedge_failed_rate": round(hedge_failed_rate, 6),
                "timeout_rate": round(timeout_rate, 6),
                "unwind_count": unwind_count,
                "skip_rate": round(skip_rate, 6),
                "avg_edge_predicted_pct": round(avg_edge_predicted_pct, 6),
                "avg_edge_captured_pct": round(avg_edge_captured_pct, 6),
                "edge_capture_ratio": round(edge_capture_ratio, 6),
                "pnl_total": round(pnl_total, 6),
                "pnl_per_trade": round(pnl_per_trade, 6),
                "max_drawdown_pct": round(max_drawdown_pct, 6),
                "p95_loss": round(p95_loss, 6),
                "p99_loss": round(p99_loss, 6),
                "go_no_go": "go" if go_no_go else "no_go",
                "go_blockers": ",".join(go_reasons),
                "robustness_score": 0.0,
            }
        )

    pnl_norm = _normalize([float(r["pnl_per_trade"]) for r in rows])
    dd_norm = _normalize([float(r["max_drawdown_pct"]) for r in rows])
    p99_norm = _normalize([float(r["p99_loss"]) for r in rows])
    for idx, row in enumerate(rows):
        score = (
            (0.25 * pnl_norm[idx])
            + (0.15 * max(0.0, min(1.0, float(row["edge_capture_ratio"]))))
            + (0.15 * (1.0 - max(0.0, min(1.0, float(row["timeout_rate"])))))
            + (0.15 * (1.0 - max(0.0, min(1.0, float(row["hedge_failed_rate"])))))
            + (0.10 * (1.0 - max(0.0, min(1.0, float(row["partial_fill_rate"])))))
            + (0.10 * (1.0 - dd_norm[idx]))
            + (0.10 * (1.0 - p99_norm[idx]))
        )
        row["robustness_score"] = round(max(0.0, min(1.0, score)), 6)
    rows.sort(key=lambda row: row["robustness_score"], reverse=True)
    return rows


def _successive_halving(events: list[SyntheticEvent], policies: list[PolicyConfig], seed: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not events:
        return _evaluate(events, policies, seed), []
    active = list(policies)
    stage = 0
    stage_meta: list[dict[str, Any]] = []
    while True:
        budget_ratio = min(1.0, 0.125 * (2**stage))
        subset_n = max(1, int(len(events) * budget_ratio))
        rows = _evaluate(events[:subset_n], active, seed + stage)
        stage_meta.append(
            {
                "stage": stage,
                "budget_ratio": round(budget_ratio, 6),
                "events_used": subset_n,
                "policies_in": len(active),
                "best_score": rows[0]["robustness_score"] if rows else 0.0,
            }
        )
        if budget_ratio >= 1.0 or len(active) <= 64:
            return rows, stage_meta
        keep_n = max(64, int(math.ceil(len(active) / 2.0)))
        active_ids = {row["policy_id"] for row in rows[:keep_n]}
        active = [p for p in active if p.policy_id in active_ids]
        stage += 1


def _write_outputs(rows: list[dict[str, Any]], out_dir: Path, *, search_mode: str, market: str, input_file: Path, stage_meta: list[dict[str, Any]]) -> tuple[Path, Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "profile_results.csv"
    json_path = out_dir / "profile_results.json"
    summary_path = out_dir / "summary.md"

    if rows:
        headers = list(rows[0].keys())
        with csv_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)
    else:
        csv_path.write_text("", encoding="utf-8")

    json_path.write_text(json.dumps(rows, indent=2, ensure_ascii=True), encoding="utf-8")

    top10 = rows[:10]
    top5 = rows[:5]
    dangerous5 = sorted(rows, key=lambda r: float(r["robustness_score"]))[:5]
    go_rows = [row for row in rows if row.get("go_no_go") == "go"]

    def _policy_line(row: dict[str, Any]) -> str:
        return (
            f"- {row['policy_id']}: score={float(row['robustness_score']):.6f}, pnl_per_trade={float(row['pnl_per_trade']):.6f}, "
            f"hedge_failed_rate={float(row['hedge_failed_rate']):.6f}, max_drawdown_pct={float(row['max_drawdown_pct']):.6f}, "
            f"p99_loss={float(row['p99_loss']):.6f}, go_no_go={row['go_no_go']}"
        )

    safe_zone_line = "- zona_segura_indisponivel: nenhum perfil passou no go/no-go"
    if go_rows:
        safe_rows = go_rows[: max(10, min(500, len(go_rows)))]
        safe_leg2 = sorted(int(float(r["leg2_timeout_ms"])) for r in safe_rows)
        safe_edge = sorted(float(r["min_edge_liq_pct"]) for r in safe_rows)
        safe_unwind = sorted(float(r["max_unwind_loss_bps"]) for r in safe_rows)
        safe_cutoff = sorted(int(float(r["entry_cutoff_sec"])) for r in safe_rows)
        safe_zone_line = (
            f"- zona_segura: leg2_timeout_ms={safe_leg2[0]}..{safe_leg2[-1]}, "
            f"min_edge_liq_pct={safe_edge[0]:.2f}..{safe_edge[-1]:.2f}, "
            f"max_unwind_loss_bps={safe_unwind[0]:.2f}..{safe_unwind[-1]:.2f}, "
            f"entry_cutoff_sec={safe_cutoff[0]}..{safe_cutoff[-1]}"
        )

    conservative = "-"
    moderate = "-"
    aggressive = "-"
    if go_rows:
        conservative_row = sorted(
            go_rows,
            key=lambda r: (
                float(r["max_drawdown_pct"]),
                float(r["p99_loss"]),
                -float(r["robustness_score"]),
            ),
        )[0]
        moderate_row = sorted(go_rows, key=lambda r: float(r["robustness_score"]), reverse=True)[0]
        aggressive_row = sorted(go_rows, key=lambda r: float(r["pnl_per_trade"]), reverse=True)[0]
        conservative = conservative_row["policy_id"]
        moderate = moderate_row["policy_id"]
        aggressive = aggressive_row["policy_id"]

    lines = [
        "# SOL5M Single-Pass Grid Summary",
        "",
        f"- market: {market}",
        f"- search_mode: {search_mode}",
        f"- input_file: {input_file.resolve()}",
        f"- policies_evaluated: {len(rows)}",
        f"- go_profiles: {len(go_rows)}",
        (
            f"- go_limits: hedge_failed_rate<={GO_HEDGE_FAILED_MAX_RATE:.4f}, pnl_per_trade>0, "
            f"max_drawdown_pct<={GO_MAX_DRAWDOWN_PCT:.4f}, p99_loss<={GO_MAX_P99_LOSS:.4f}"
        ),
        "",
        "## Top 10 policies por robustez",
    ]
    lines.append("| policy_id | score | pnl_per_trade | hedge_failed_rate | max_drawdown_pct | p99_loss | go_no_go |")
    lines.append("|---|---:|---:|---:|---:|---:|---|")
    for row in top10:
        lines.append(
            f"| {row['policy_id']} | {float(row['robustness_score']):.6f} | {float(row['pnl_per_trade']):.6f} | "
            f"{float(row['hedge_failed_rate']):.6f} | {float(row['max_drawdown_pct']):.6f} | "
            f"{float(row['p99_loss']):.6f} | {row['go_no_go']} |"
        )
    lines.append("")
    lines.append("## Top perfis (Top 5)")
    for row in top5:
        lines.append(_policy_line(row))
    lines.append("")
    lines.append("## Perfis perigosos (Bottom 5)")
    for row in dangerous5:
        lines.append(_policy_line(row))
    lines.append("")
    lines.append("## Zona segura sugerida")
    lines.append(safe_zone_line)
    lines.append("")
    lines.append("## Recomendacao final")
    lines.append(f"- conservador: {conservative}")
    lines.append(f"- moderado: {moderate}")
    lines.append(f"- agressivo: {aggressive}")
    if stage_meta:
        lines.append("")
        lines.append("## Successive Halving Stages")
        for stage in stage_meta:
            lines.append(
                f"- stage={stage['stage']} policies_in={stage['policies_in']} events_used={stage['events_used']} "
                f"budget_ratio={stage['budget_ratio']} best_score={stage['best_score']}"
            )
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return csv_path, json_path, summary_path


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    input_path = Path(args.input)
    grid_path = Path(args.grid_file)
    out_dir = Path(args.out_dir)

    events = _load_replay_events(input_path=input_path, market=args.market)
    policies = _load_grid(grid_file=grid_path)

    if args.search_mode == "full":
        rows = _evaluate(events=events, policies=policies, seed=int(args.seed))
        stage_meta: list[dict[str, Any]] = []
    else:
        rows, stage_meta = _successive_halving(events=events, policies=policies, seed=int(args.seed))

    csv_path, json_path, summary_path = _write_outputs(
        rows=rows,
        out_dir=out_dir,
        search_mode=args.search_mode,
        market=args.market,
        input_file=input_path,
        stage_meta=stage_meta,
    )

    print(f"market={args.market}")
    print(f"events_loaded={len(events)}")
    print(f"search_mode={args.search_mode}")
    print(f"policies_output={len(rows)}")
    print(f"csv_file={csv_path.resolve()}")
    print(f"json_file={json_path.resolve()}")
    print(f"summary_file={summary_path.resolve()}")
    print("top5_policy_ids=" + ",".join(row["policy_id"] for row in rows[:5]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
