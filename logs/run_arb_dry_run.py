import argparse
import importlib.util
import subprocess
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    # Legacy entrypoint compatibility: allow importing the new package layout.
    sys.path.insert(0, str(_REPO_ROOT))

from live_direct_arb import run_live_mode

try:
    from scripts.crypto_cli import validate_from_namespace as _validate_from_arb_cli
except Exception:
    _validate_from_arb_cli = None
    try:
        _ARB_CLI_PATH = Path(__file__).resolve().parent.parent / "scripts" / "crypto_cli.py"
        if not _ARB_CLI_PATH.exists():
            _ARB_CLI_PATH = Path(__file__).resolve().parent.parent / "scripts" / "arb_cli.py"
        if _ARB_CLI_PATH.exists():
            spec = importlib.util.spec_from_file_location("scripts.crypto_cli", str(_ARB_CLI_PATH))
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                _validate_from_arb_cli = getattr(module, "validate_from_namespace", None)
    except Exception:
        _validate_from_arb_cli = None


def parse_args():
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent

    parser = argparse.ArgumentParser(description="Dry run runner for BTC 15m arbitrage pipeline.")
    parser.add_argument(
        "--mode",
        choices=[
            "replay",
            "live-observe",
            "live-sim",
            "live-shadow",
            "live-prod",
            "fault-injection",
            "pessimistic-exec",
            "production-ready",
        ],
        default="replay",
        help="Dry run mode.",
    )
    parser.add_argument("--python-exe", default=sys.executable, help="Python executable.")
    parser.add_argument("--kalshi-file", default=str(script_dir / "kalshi_btc_15m_ticks.csv"))
    parser.add_argument("--poly-file", default=str(script_dir / "poly_btc_15m_ticks.csv"))
    parser.add_argument("--output-dir", default=str(script_dir))
    parser.add_argument("--summary-file", default=str(script_dir / "dry_run_summary.txt"))
    parser.add_argument("--analyzer", default=str(script_dir / "analyze_arb.py"))
    parser.add_argument("--watch-kalshi", default=str(repo_root / "watch_btc_15m_kalshi.py"))
    parser.add_argument("--watch-poly", default=str(repo_root / "watch_btc_15m_poly.py"))
    parser.add_argument("--tolerance-sec", type=float, default=1.0)
    parser.add_argument("--fee-poly-bps", type=float, default=25.0)
    parser.add_argument("--fee-kalshi-bps", type=float, default=0.0)
    parser.add_argument("--min-edge-pct", type=float, default=5.0)
    parser.add_argument("--slippage-expected-bps", type=float, default=0.0)
    parser.add_argument("--leg-risk-cost", type=float, default=0.0)
    parser.add_argument("--payout-esperado", type=float, default=1.0)
    parser.add_argument("--min-liquidity", type=float, default=1.0)
    parser.add_argument("--max-losses-streak", type=int, default=3)
    parser.add_argument("--max-daily-drawdown-pct", type=float, default=20.0)
    parser.add_argument("--max-open-positions", type=int, default=1)
    parser.add_argument("--kill-switch-path", default="logs/kill_switch.flag")
    parser.add_argument("--leg-timeout-sec", type=float, default=2.0)
    parser.add_argument("--hedge-timeout-sec", type=float, default=2.0)
    parser.add_argument("--exec-sim-partial-fill-ratio", type=float, default=1.0)
    parser.add_argument("--exec-sim-leg-latency-sec", type=float, default=0.0)
    parser.add_argument("--exec-force-hedge-fail", default="false", choices=["true", "false"])
    parser.add_argument("--live-duration-sec", type=int, default=60)
    parser.add_argument("--live-interval-sec", type=float, default=0.5)
    parser.add_argument("--pess-delay-sec", type=float, default=5.0)
    parser.add_argument("--pess-cancel-timeout-rate", type=float, default=0.40)
    parser.add_argument("--pess-cancel-timeout-penalty", type=float, default=0.01)
    parser.add_argument("--pess-late-fill-rate", type=float, default=0.40)
    parser.add_argument("--pess-late-fill-penalty", type=float, default=0.02)
    parser.add_argument("--pess-reprice-rate", type=float, default=0.14)
    parser.add_argument("--pess-reprice-penalty", type=float, default=0.07)
    parser.add_argument("--pess-tail-rate", type=float, default=0.03)
    parser.add_argument("--pess-tail-penalty", type=float, default=0.20)
    parser.add_argument("--prod-max-usd-kalshi", type=float, default=10.0)
    parser.add_argument("--prod-max-usd-poly", type=float, default=10.0)
    parser.add_argument("--prod-min-edge-pct", type=float, default=7.0)
    parser.add_argument("--prod-min-tte-sec", type=float, default=180.0)
    parser.add_argument("--prod-share-step", type=float, default=1.0)
    parser.add_argument("--prod-min-shares", type=float, default=1.0)
    parser.add_argument("--prod-max-shares", type=float, default=100.0)
    parser.add_argument("--prod-max-open-trades", type=int, default=1)
    parser.add_argument("--prod-fail-if-empty", action="store_true")
    parser.add_argument("--runtime-sec", type=float, default=120.0)
    parser.add_argument("--eval-interval-sec", type=float, default=0.2)
    parser.add_argument("--max-usd-kalshi", type=float, default=10.0)
    parser.add_argument("--max-usd-poly", type=float, default=10.0)
    parser.add_argument("--max-open-trades", type=int, default=1)
    parser.add_argument("--max-shares-per-trade", type=int, default=100)
    parser.add_argument("--post-only-strict", default="true", choices=["true", "false"])
    parser.add_argument("--nonce-guard", default="on", choices=["on", "off"])
    parser.add_argument("--nonce-guard-action", default="alert", choices=["alert"])
    parser.add_argument("--poly-feed", default="ws", choices=["ws"])
    parser.add_argument("--kalshi-feed", default="rest", choices=["rest"])
    parser.add_argument("--nonce-poll-sec", type=float, default=2.0)
    parser.add_argument("--kalshi-poll-sec", type=float, default=0.25)
    parser.add_argument("--poly-market-refresh-sec", type=float, default=1.0)
    parser.add_argument("--outcome-poll-grace-sec", type=float, default=5.0)
    parser.add_argument("--kalshi-series", default="KXBTC15M")
    parser.add_argument("--live-trades-csv", default=str(script_dir / "arb_live_trades.csv"))
    parser.add_argument("--live-decisions-csv", default=str(script_dir / "arb_live_decisions.csv"))
    parser.add_argument("--live-security-csv", default=str(script_dir / "arb_live_security.csv"))
    parser.add_argument("--live-summary-file", default=str(script_dir / "arb_live_summary.txt"))
    parser.add_argument("--sqlite-file", default=str(script_dir / "arb_runtime.sqlite"))
    parser.add_argument("--jsonl-log-file", default=str(script_dir / "arb_events.jsonl"))
    parser.add_argument("--execution-mode", default="paper", choices=["paper", "live"])
    parser.add_argument("--live-confirmation", default="")
    parser.add_argument("--enable-live-prod", action="store_true")
    parser.add_argument("--kalshi-order-live", default="false", choices=["true", "false"])
    parser.add_argument("--allow-single-leg-risk", default="false", choices=["true", "false"])
    parser.add_argument("--kalshi-api-key-id", default="")
    parser.add_argument("--kalshi-private-key-path", default="")
    parser.add_argument("--kalshi-base-url", default="https://api.elections.kalshi.com/trade-api/v2")
    parser.add_argument("--kalshi-order-timeout-sec", type=float, default=10.0)
    parser.add_argument(
        "--kalshi-time-in-force",
        default="good_till_canceled",
        choices=["good_till_canceled", "immediate_or_cancel", "fill_or_kill"],
    )
    parser.add_argument("--kalshi-sign-path-mode", default="auto", choices=["auto", "with_base", "without_base"])
    return parser.parse_args()


def run_cmd(cmd: list[str], cwd: Path):
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
    )


def run_analyzer(
    python_exe: str,
    analyzer_path: Path,
    cwd: Path,
    kalshi_file: Path,
    poly_file: Path,
    opps_csv: Path,
    diag_csv: Path,
    summary_file: Path,
    tolerance_sec: float,
    fee_poly_bps: float,
    fee_kalshi_bps: float,
    min_edge_pct: float,
    slippage_expected_bps: float = 0.0,
    leg_risk_cost: float = 0.0,
    payout_expected: float = 1.0,
):
    cmd = [
        python_exe,
        str(analyzer_path),
        "--kalshi-file",
        str(kalshi_file),
        "--poly-file",
        str(poly_file),
        "--opps-csv",
        str(opps_csv),
        "--diag-csv",
        str(diag_csv),
        "--summary-file",
        str(summary_file),
        "--tolerance-sec",
        str(tolerance_sec),
        "--fee-poly-bps",
        str(fee_poly_bps),
        "--fee-kalshi-bps",
        str(fee_kalshi_bps),
        "--min-edge-pct",
        str(min_edge_pct),
        "--slippage-expected-bps",
        str(slippage_expected_bps),
        "--leg-risk-cost",
        str(leg_risk_cost),
        "--payout-expected",
        str(payout_expected),
    ]
    return run_cmd(cmd, cwd=cwd)


def scenario_schema_drift(k: pd.DataFrame, p: pd.DataFrame):
    out_k = k.copy()
    out_p = p.copy()
    if "yes_ask" in out_k.columns:
        out_k = out_k.rename(columns={"yes_ask": "yes_ask_legacy"})
    return out_k, out_p


def scenario_type_mismatch(k: pd.DataFrame, p: pd.DataFrame):
    out_k = k.copy()
    out_p = p.copy()
    if "yes_ask" not in out_k.columns:
        out_k["yes_ask"] = ""
    out_k["yes_ask"] = out_k["yes_ask"].astype("object")
    if len(out_k) > 0:
        out_k.loc[out_k.index[0], "yes_ask"] = "BROKEN_VALUE"
    return out_k, out_p


def scenario_ws_disconnect_timeout(k: pd.DataFrame, p: pd.DataFrame):
    out_k = k.copy()
    out_p = p.copy()
    for frame in (out_k, out_p):
        if "row_status" not in frame.columns:
            frame["row_status"] = "valid"
        if "error_code" not in frame.columns:
            frame["error_code"] = ""
        if len(frame) > 0:
            n = min(20, len(frame))
            frame.loc[frame.index[:n], "row_status"] = "invalid"
            frame.loc[frame.index[:n], "error_code"] = "ws_disconnect_timeout"
    return out_k, out_p


def scenario_market_rollover_race(k: pd.DataFrame, p: pd.DataFrame):
    out_k = k.copy()
    out_p = p.copy()
    if "market_key" not in out_p.columns:
        out_p["market_key"] = ""
    if len(out_p) > 0:
        n = min(20, len(out_p))
        out_p.loc[out_p.index[:n], "market_key"] = "BTC15M_2099-01-01T00:00:00Z"
    return out_k, out_p


def scenario_stale_quotes(k: pd.DataFrame, p: pd.DataFrame):
    out_k = k.copy()
    out_p = p.copy()
    if "timestamp_utc" in out_p.columns:
        ts = pd.to_datetime(out_p["timestamp_utc"], errors="coerce", utc=True) + pd.Timedelta(seconds=15)
        out_p["timestamp_utc"] = ts.dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return out_k, out_p


def scenario_join_inflation(k: pd.DataFrame, p: pd.DataFrame):
    out_k = k.copy()
    out_p = p.copy()
    if "timestamp_utc" in out_k.columns and len(out_k) > 0:
        dup = out_k.head(min(200, len(out_k))).copy()
        ts = pd.to_datetime(dup["timestamp_utc"], errors="coerce", utc=True) + pd.Timedelta(milliseconds=150)
        dup["timestamp_utc"] = ts.dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        out_k = pd.concat([out_k, dup], ignore_index=True)
    return out_k, out_p


def scenario_missing_book_side(k: pd.DataFrame, p: pd.DataFrame):
    out_k = k.copy()
    out_p = p.copy()
    if "down_best_ask" not in out_p.columns:
        out_p["down_best_ask"] = ""
    out_p["down_best_ask"] = out_p["down_best_ask"].astype("object")
    if len(out_p) > 0:
        n = min(30, len(out_p))
        out_p.loc[out_p.index[:n], "down_best_ask"] = ""
    return out_k, out_p


def _clamp_rate(v: float) -> float:
    return max(0.0, min(1.0, float(v)))


def _prepare_kalshi_quotes(df: pd.DataFrame):
    out = df.copy()
    if "row_status" in out.columns:
        out = out[out["row_status"].fillna("invalid") == "valid"].copy()
    if "timestamp_utc" not in out.columns or "market_key" not in out.columns:
        return pd.DataFrame(columns=["market_key", "timestamp_utc", "yes_ask", "no_ask"])
    if "yes_ask" not in out.columns:
        out["yes_ask"] = np.nan
    if "no_ask" not in out.columns:
        out["no_ask"] = np.nan
    out["timestamp_utc"] = pd.to_datetime(out["timestamp_utc"], utc=True, errors="coerce")
    out["yes_ask"] = pd.to_numeric(out["yes_ask"], errors="coerce")
    out["no_ask"] = pd.to_numeric(out["no_ask"], errors="coerce")
    out = out.dropna(subset=["market_key", "timestamp_utc"]).copy()
    out["timestamp_utc"] = out["timestamp_utc"].dt.tz_convert("UTC").dt.tz_localize(None)
    return out[["market_key", "timestamp_utc", "yes_ask", "no_ask"]].sort_values("timestamp_utc")


def _prepare_poly_quotes(df: pd.DataFrame):
    out = df.copy()
    if "row_status" in out.columns:
        out = out[out["row_status"].fillna("invalid") == "valid"].copy()
    if "timestamp_utc" not in out.columns or "market_key" not in out.columns:
        return pd.DataFrame(columns=["market_key", "timestamp_utc", "up_best_ask", "down_best_ask"])
    if "up_best_ask" not in out.columns:
        out["up_best_ask"] = np.nan
    if "down_best_ask" not in out.columns:
        out["down_best_ask"] = np.nan
    out["timestamp_utc"] = pd.to_datetime(out["timestamp_utc"], utc=True, errors="coerce")
    out["up_best_ask"] = pd.to_numeric(out["up_best_ask"], errors="coerce")
    out["down_best_ask"] = pd.to_numeric(out["down_best_ask"], errors="coerce")
    out = out.dropna(subset=["market_key", "timestamp_utc"]).copy()
    out["timestamp_utc"] = out["timestamp_utc"].dt.tz_convert("UTC").dt.tz_localize(None)
    return out[["market_key", "timestamp_utc", "up_best_ask", "down_best_ask"]].sort_values("timestamp_utc")


def _build_quote_lookup(df: pd.DataFrame, leg_a: str, leg_b: str):
    lookup = {}
    if df.empty:
        return lookup
    for market_key, g in df.groupby("market_key", sort=False):
        lookup[str(market_key)] = {
            "ts": g["timestamp_utc"].to_numpy(dtype="datetime64[ns]"),
            leg_a: g[leg_a].to_numpy(dtype="float64"),
            leg_b: g[leg_b].to_numpy(dtype="float64"),
        }
    return lookup


def _lookup_at_or_after(ts_arr, val_arr, target_ts):
    idx = np.searchsorted(ts_arr, target_ts.to_datetime64(), side="left")
    if idx >= len(ts_arr):
        return np.nan
    return val_arr[idx]


def _edge_from_cost(cost: float) -> float:
    if cost <= 0:
        return -9999.0
    return ((1.0 - cost) / cost) * 100.0


def _safe_ts(row) -> pd.Timestamp | None:
    for col in ("sec", "timestamp_utc_k", "timestamp_utc_p"):
        if col in row and pd.notna(row[col]):
            ts = pd.to_datetime(row[col], utc=True, errors="coerce")
            if pd.notna(ts):
                return ts
    return None


def _latest_file(pattern: str, directory: Path) -> Path | None:
    files = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime)
    return files[-1] if files else None


def _to_bool_series(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series
    lowered = series.astype(str).str.strip().str.lower()
    return lowered.isin({"1", "true", "yes", "y"})


def _market_close_from_key(series: pd.Series) -> pd.Series:
    close_raw = series.astype(str).str.extract(r"BTC15M_(.+)$")[0]
    return pd.to_datetime(close_raw, utc=True, errors="coerce")


def _floor_to_step(value: float, step: float) -> float:
    s = max(0.0, float(step))
    if s <= 0:
        return max(0.0, float(value))
    return max(0.0, np.floor(float(value) / s) * s)


def _build_production_plan(stress_df: pd.DataFrame, args) -> tuple[pd.DataFrame, dict]:
    stats = {
        "rows_raw": int(len(stress_df)),
        "rows_after_survive": 0,
        "rows_after_edge": 0,
        "rows_after_tte": 0,
        "rows_after_price": 0,
        "rows_after_dedupe": 0,
        "rows_budget_blocked": 0,
        "rows_shares_blocked": 0,
    }

    required = [
        "sec",
        "market_key",
        "strategy",
        "base_kalshi_leg_price",
        "base_poly_leg_price",
        "ops_expected_cost",
        "ops_expected_edge_pct",
        "survive_ops_expected",
    ]
    missing = [c for c in required if c not in stress_df.columns]
    if missing:
        stats["missing_columns"] = ",".join(missing)
        return pd.DataFrame(), stats

    df = stress_df.copy()
    df["sec_ts"] = pd.to_datetime(df["sec"], utc=True, errors="coerce")
    df["close_ts"] = _market_close_from_key(df["market_key"])
    df["tte_sec"] = (df["close_ts"] - df["sec_ts"]).dt.total_seconds()
    df["survive_ops_expected"] = _to_bool_series(df["survive_ops_expected"])
    df["ops_expected_edge_pct"] = pd.to_numeric(df["ops_expected_edge_pct"], errors="coerce")
    df["ops_expected_cost"] = pd.to_numeric(df["ops_expected_cost"], errors="coerce")
    df["base_kalshi_leg_price"] = pd.to_numeric(df["base_kalshi_leg_price"], errors="coerce")
    df["base_poly_leg_price"] = pd.to_numeric(df["base_poly_leg_price"], errors="coerce")
    df["base_edge_pct"] = pd.to_numeric(df.get("base_edge_pct", np.nan), errors="coerce")
    df["delayed_edge_pct"] = pd.to_numeric(df.get("delayed_edge_pct", np.nan), errors="coerce")
    df["unit_expected_profit"] = 1.0 - df["ops_expected_cost"]

    df = df[df["survive_ops_expected"]].copy()
    stats["rows_after_survive"] = int(len(df))
    if df.empty:
        return pd.DataFrame(), stats

    df = df[df["ops_expected_edge_pct"] >= float(args.prod_min_edge_pct)].copy()
    stats["rows_after_edge"] = int(len(df))
    if df.empty:
        return pd.DataFrame(), stats

    df = df[df["ops_expected_cost"] < 1.0].copy()
    df = df[df["tte_sec"] >= float(args.prod_min_tte_sec)].copy()
    stats["rows_after_tte"] = int(len(df))
    if df.empty:
        return pd.DataFrame(), stats

    df = df[(df["base_kalshi_leg_price"] > 0.0) & (df["base_poly_leg_price"] > 0.0)].copy()
    stats["rows_after_price"] = int(len(df))
    if df.empty:
        return pd.DataFrame(), stats

    df = (
        df.sort_values(
            ["unit_expected_profit", "ops_expected_edge_pct", "sec_ts"],
            ascending=[False, False, False],
        )
        .drop_duplicates(["market_key", "strategy"], keep="first")
        .reset_index(drop=True)
    )
    stats["rows_after_dedupe"] = int(len(df))
    if df.empty:
        return pd.DataFrame(), stats

    fee_k = float(args.fee_kalshi_bps) / 10000.0
    fee_p = float(args.fee_poly_bps) / 10000.0
    remaining_k = max(0.0, float(args.prod_max_usd_kalshi))
    remaining_p = max(0.0, float(args.prod_max_usd_poly))
    max_shares = max(0.0, float(args.prod_max_shares))
    min_shares = max(0.0, float(args.prod_min_shares))
    share_step = max(0.0, float(args.prod_share_step))
    max_trades = max(1, int(args.prod_max_open_trades))

    plan_rows = []
    for _, row in df.iterrows():
        if len(plan_rows) >= max_trades:
            break

        k_px = float(row["base_kalshi_leg_price"])
        p_px = float(row["base_poly_leg_price"])
        if k_px <= 0 or p_px <= 0:
            stats["rows_budget_blocked"] += 1
            continue

        raw_by_budget = min(remaining_k / k_px, remaining_p / p_px, max_shares)
        if raw_by_budget <= 0:
            stats["rows_budget_blocked"] += 1
            continue

        shares = _floor_to_step(raw_by_budget, share_step)
        if shares < min_shares:
            stats["rows_shares_blocked"] += 1
            continue

        k_notional = shares * k_px
        p_notional = shares * p_px
        if k_notional <= 0 or p_notional <= 0:
            stats["rows_shares_blocked"] += 1
            continue

        k_fee = k_notional * fee_k
        p_fee = p_notional * fee_p
        total_cost = k_notional + p_notional + k_fee + p_fee
        payout = shares
        expected_profit = payout - total_cost
        expected_roi = ((expected_profit / total_cost) * 100.0) if total_cost > 0 else np.nan

        remaining_k = max(0.0, remaining_k - (k_notional + k_fee))
        remaining_p = max(0.0, remaining_p - (p_notional + p_fee))

        cap_binding = "kalshi" if (k_notional / max(1e-12, float(args.prod_max_usd_kalshi))) >= (
            p_notional / max(1e-12, float(args.prod_max_usd_poly))
        ) else "poly"

        plan_rows.append(
            {
                "sec": row["sec"],
                "market_key": row["market_key"],
                "strategy": row["strategy"],
                "tte_sec": row["tte_sec"],
                "base_edge_pct": row["base_edge_pct"],
                "delayed_edge_pct": row["delayed_edge_pct"],
                "ops_expected_edge_pct": row["ops_expected_edge_pct"],
                "ops_expected_cost": row["ops_expected_cost"],
                "kalshi_leg_price": k_px,
                "poly_leg_price": p_px,
                "shares_planned": shares,
                "kalshi_notional_usd": k_notional,
                "poly_notional_usd": p_notional,
                "kalshi_fee_usd": k_fee,
                "poly_fee_usd": p_fee,
                "total_cost_usd": total_cost,
                "payout_usd_if_hedged": payout,
                "expected_profit_usd": expected_profit,
                "expected_roi_pct": expected_roi,
                "cap_binding_side": cap_binding,
                "remaining_kalshi_budget_usd": remaining_k,
                "remaining_poly_budget_usd": remaining_p,
            }
        )

    plan_df = pd.DataFrame(plan_rows)
    return plan_df, stats


def run_pessimistic_exec(args, repo_root: Path, output_dir: Path, summary_lines: list[str]) -> int:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base_opps_csv = output_dir / f"arb_opportunities_pessbase_{ts}.csv"
    base_diag_csv = output_dir / f"arb_diagnostics_pessbase_{ts}.csv"
    base_analysis_summary = output_dir / f"arb_analysis_pessbase_{ts}.txt"
    stress_csv = output_dir / f"arb_pessimistic_exec_{ts}.csv"

    summary_lines.append("[pessimistic-exec]")
    summary_lines.append(f"pess_delay_sec={args.pess_delay_sec}")
    summary_lines.append(f"pess_cancel_timeout_rate={args.pess_cancel_timeout_rate}")
    summary_lines.append(f"pess_cancel_timeout_penalty={args.pess_cancel_timeout_penalty}")
    summary_lines.append(f"pess_late_fill_rate={args.pess_late_fill_rate}")
    summary_lines.append(f"pess_late_fill_penalty={args.pess_late_fill_penalty}")
    summary_lines.append(f"pess_reprice_rate={args.pess_reprice_rate}")
    summary_lines.append(f"pess_reprice_penalty={args.pess_reprice_penalty}")
    summary_lines.append(f"pess_tail_rate={args.pess_tail_rate}")
    summary_lines.append(f"pess_tail_penalty={args.pess_tail_penalty}")

    analyzer_res = run_analyzer(
        args.python_exe,
        Path(args.analyzer),
        repo_root,
        Path(args.kalshi_file).resolve(),
        Path(args.poly_file).resolve(),
        base_opps_csv,
        base_diag_csv,
        base_analysis_summary,
        args.tolerance_sec,
        args.fee_poly_bps,
        args.fee_kalshi_bps,
        args.min_edge_pct,
        args.slippage_expected_bps,
        args.leg_risk_cost,
        args.payout_esperado,
    )

    summary_lines.append(f"analyzer_rc={analyzer_res.returncode}")
    summary_lines.append(f"base_opps_csv={base_opps_csv}")
    summary_lines.append(f"base_diag_csv={base_diag_csv}")
    if analyzer_res.stdout.strip():
        summary_lines.append("analyzer_stdout:")
        summary_lines.extend([f"  {line}" for line in analyzer_res.stdout.strip().splitlines()])
    if analyzer_res.stderr.strip():
        summary_lines.append("analyzer_stderr:")
        summary_lines.extend([f"  {line}" for line in analyzer_res.stderr.strip().splitlines()])
    if analyzer_res.returncode != 0:
        return 1

    if not base_opps_csv.exists():
        summary_lines.append("base_opportunities_missing=true")
        return 1

    opps = pd.read_csv(base_opps_csv)
    if opps.empty:
        summary_lines.append("base_opportunities_rows=0")
        summary_lines.append(f"stress_csv={stress_csv}")
        pd.DataFrame(
            columns=[
                "sec",
                "market_key",
                "strategy",
                "base_cost",
                "base_edge_pct",
                "delayed_cost",
                "delayed_edge_pct",
                "ops_expected_add",
                "ops_expected_cost",
                "ops_expected_edge_pct",
                "ops_worst_add",
                "ops_worst_cost",
                "ops_worst_edge_pct",
                "survive_delay_only",
                "survive_ops_expected",
                "survive_ops_worst",
                "drop_reason_expected",
                "drop_reason_worst",
            ]
        ).to_csv(stress_csv, index=False)
        return 0

    kalshi = pd.read_csv(Path(args.kalshi_file).resolve())
    poly = pd.read_csv(Path(args.poly_file).resolve())
    kq = _prepare_kalshi_quotes(kalshi)
    pq = _prepare_poly_quotes(poly)
    k_lookup = _build_quote_lookup(kq, "yes_ask", "no_ask")
    p_lookup = _build_quote_lookup(pq, "up_best_ask", "down_best_ask")

    fee_k = float(args.fee_kalshi_bps) / 10000.0
    fee_p = float(args.fee_poly_bps) / 10000.0
    min_edge = float(args.min_edge_pct)
    target_cost = 1.0 / (1.0 + (min_edge / 100.0))
    delay = float(args.pess_delay_sec)

    cancel_rate = _clamp_rate(args.pess_cancel_timeout_rate)
    late_rate = _clamp_rate(args.pess_late_fill_rate)
    reprice_rate = _clamp_rate(args.pess_reprice_rate)
    tail_rate = _clamp_rate(args.pess_tail_rate)
    cancel_pen = max(0.0, float(args.pess_cancel_timeout_penalty))
    late_pen = max(0.0, float(args.pess_late_fill_penalty))
    reprice_pen = max(0.0, float(args.pess_reprice_penalty))
    tail_pen = max(0.0, float(args.pess_tail_penalty))

    ops_expected_add = (cancel_rate * cancel_pen) + (late_rate * late_pen) + (reprice_rate * reprice_pen) + (
        tail_rate * tail_pen
    )
    ops_worst_add = 0.0
    for rate, pen in ((cancel_rate, cancel_pen), (late_rate, late_pen), (reprice_rate, reprice_pen), (tail_rate, tail_pen)):
        if rate > 0 and pen > 0:
            ops_worst_add += pen

    rows = []
    missing_future_quote = 0
    unknown_strategy = 0

    for _, row in opps.iterrows():
        market_key = str(row.get("market_key", ""))
        strategy = str(row.get("strategy", ""))
        ts_ref = _safe_ts(row)
        if ts_ref is None:
            missing_future_quote += 1
            continue

        base_k_leg = pd.to_numeric(pd.Series([row.get("kalshi_leg_price", np.nan)]), errors="coerce").iloc[0]
        base_p_leg = pd.to_numeric(pd.Series([row.get("poly_leg_price", np.nan)]), errors="coerce").iloc[0]
        if pd.isna(base_k_leg) or pd.isna(base_p_leg):
            missing_future_quote += 1
            continue

        target_ts = (ts_ref + pd.Timedelta(seconds=delay)).tz_convert("UTC").tz_localize(None)
        k_entry = k_lookup.get(market_key)
        p_entry = p_lookup.get(market_key)
        if k_entry is None or p_entry is None:
            missing_future_quote += 1
            continue

        if strategy.startswith("A_"):
            delayed_k = _lookup_at_or_after(k_entry["ts"], k_entry["yes_ask"], target_ts)
            delayed_p = _lookup_at_or_after(p_entry["ts"], p_entry["down_best_ask"], target_ts)
        elif strategy.startswith("B_"):
            delayed_k = _lookup_at_or_after(k_entry["ts"], k_entry["no_ask"], target_ts)
            delayed_p = _lookup_at_or_after(p_entry["ts"], p_entry["up_best_ask"], target_ts)
        else:
            unknown_strategy += 1
            continue

        if pd.isna(delayed_k) or pd.isna(delayed_p):
            missing_future_quote += 1
            continue

        # Pessimistic assumption: delayed execution does not improve either leg.
        stressed_k = max(float(base_k_leg), float(delayed_k))
        stressed_p = max(float(base_p_leg), float(delayed_p))

        delayed_cost = (stressed_k * (1.0 + fee_k)) + (stressed_p * (1.0 + fee_p))
        delayed_edge = _edge_from_cost(delayed_cost)
        expected_cost = delayed_cost + ops_expected_add
        expected_edge = _edge_from_cost(expected_cost)
        worst_cost = delayed_cost + ops_worst_add
        worst_edge = _edge_from_cost(worst_cost)

        survive_delay = (delayed_cost < target_cost) and (delayed_edge >= min_edge)
        survive_expected = (expected_cost < target_cost) and (expected_edge >= min_edge)
        survive_worst = (worst_cost < target_cost) and (worst_edge >= min_edge)

        if not survive_delay:
            reason_expected = "delay_decay_below_threshold"
            reason_worst = "delay_decay_below_threshold"
        elif not survive_expected:
            reason_expected = "ops_expected_decay_below_threshold"
            reason_worst = "ops_worst_decay_below_threshold" if not survive_worst else "survive"
        else:
            reason_expected = "survive"
            reason_worst = "ops_worst_decay_below_threshold" if not survive_worst else "survive"

        rows.append(
            {
                "sec": row.get("sec"),
                "market_key": market_key,
                "strategy": strategy,
                "base_cost": row.get("cost"),
                "base_edge_pct": row.get("edge_pct"),
                "base_kalshi_leg_price": base_k_leg,
                "base_poly_leg_price": base_p_leg,
                "delayed_kalshi_leg_price": stressed_k,
                "delayed_poly_leg_price": stressed_p,
                "delayed_cost": delayed_cost,
                "delayed_edge_pct": delayed_edge,
                "ops_expected_add": ops_expected_add,
                "ops_expected_cost": expected_cost,
                "ops_expected_edge_pct": expected_edge,
                "ops_worst_add": ops_worst_add,
                "ops_worst_cost": worst_cost,
                "ops_worst_edge_pct": worst_edge,
                "survive_delay_only": survive_delay,
                "survive_ops_expected": survive_expected,
                "survive_ops_worst": survive_worst,
                "drop_reason_expected": reason_expected,
                "drop_reason_worst": reason_worst,
            }
        )

    stress_df = pd.DataFrame(rows)
    stress_df.to_csv(stress_csv, index=False)
    summary_lines.append(f"stress_csv={stress_csv}")
    summary_lines.append(f"base_opportunities_rows={len(opps)}")
    summary_lines.append(f"stress_rows={len(stress_df)}")
    summary_lines.append(f"missing_future_quote_rows={missing_future_quote}")
    summary_lines.append(f"unknown_strategy_rows={unknown_strategy}")
    summary_lines.append(f"target_cost_for_min_edge={target_cost:.6f}")
    summary_lines.append(f"ops_expected_add={ops_expected_add:.6f}")
    summary_lines.append(f"ops_worst_add={ops_worst_add:.6f}")

    if not stress_df.empty:
        delay_survive = int(stress_df["survive_delay_only"].sum())
        expected_survive = int(stress_df["survive_ops_expected"].sum())
        worst_survive = int(stress_df["survive_ops_worst"].sum())
        total = len(stress_df)
        summary_lines.append(f"survive_delay_only={delay_survive}/{total} ({(100.0 * delay_survive / total):.2f}%)")
        summary_lines.append(
            f"survive_ops_expected={expected_survive}/{total} ({(100.0 * expected_survive / total):.2f}%)"
        )
        summary_lines.append(f"survive_ops_worst={worst_survive}/{total} ({(100.0 * worst_survive / total):.2f}%)")
        summary_lines.append(f"delayed_edge_median={stress_df['delayed_edge_pct'].median():.4f}")
        summary_lines.append(f"ops_expected_edge_median={stress_df['ops_expected_edge_pct'].median():.4f}")
        summary_lines.append(f"ops_worst_edge_median={stress_df['ops_worst_edge_pct'].median():.4f}")
        summary_lines.append("drop_reason_expected_counts:")
        for reason, count in stress_df["drop_reason_expected"].value_counts().items():
            summary_lines.append(f"  - {reason}: {int(count)}")
        summary_lines.append("drop_reason_worst_counts:")
        for reason, count in stress_df["drop_reason_worst"].value_counts().items():
            summary_lines.append(f"  - {reason}: {int(count)}")

    return 0


def run_production_ready(args, repo_root: Path, output_dir: Path, summary_lines: list[str]) -> int:
    summary_lines.append("[production-ready]")
    summary_lines.append(f"prod_max_usd_kalshi={args.prod_max_usd_kalshi}")
    summary_lines.append(f"prod_max_usd_poly={args.prod_max_usd_poly}")
    summary_lines.append(f"prod_min_edge_pct={args.prod_min_edge_pct}")
    summary_lines.append(f"prod_min_tte_sec={args.prod_min_tte_sec}")
    summary_lines.append(f"prod_share_step={args.prod_share_step}")
    summary_lines.append(f"prod_min_shares={args.prod_min_shares}")
    summary_lines.append(f"prod_max_shares={args.prod_max_shares}")
    summary_lines.append(f"prod_max_open_trades={args.prod_max_open_trades}")
    summary_lines.append(f"prod_fail_if_empty={args.prod_fail_if_empty}")

    rc = run_pessimistic_exec(args, repo_root, output_dir, summary_lines)
    if rc != 0:
        summary_lines.append("production_ready=false")
        summary_lines.append("production_reason=pessimistic_exec_failed")
        return 1

    stress_csv = _latest_file("arb_pessimistic_exec_*.csv", output_dir)
    if stress_csv is None or not stress_csv.exists():
        summary_lines.append("production_ready=false")
        summary_lines.append("production_reason=missing_stress_csv")
        return 1

    stress_df = pd.read_csv(stress_csv)
    plan_df, stats = _build_production_plan(stress_df, args)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    plan_csv = output_dir / f"arb_production_plan_{ts}.csv"
    plan_df.to_csv(plan_csv, index=False)

    summary_lines.append(f"stress_source_csv={stress_csv}")
    summary_lines.append(f"production_plan_csv={plan_csv}")
    summary_lines.append(f"production_rows_raw={stats.get('rows_raw', 0)}")
    summary_lines.append(f"production_rows_after_survive={stats.get('rows_after_survive', 0)}")
    summary_lines.append(f"production_rows_after_edge={stats.get('rows_after_edge', 0)}")
    summary_lines.append(f"production_rows_after_tte={stats.get('rows_after_tte', 0)}")
    summary_lines.append(f"production_rows_after_price={stats.get('rows_after_price', 0)}")
    summary_lines.append(f"production_rows_after_dedupe={stats.get('rows_after_dedupe', 0)}")
    summary_lines.append(f"production_rows_budget_blocked={stats.get('rows_budget_blocked', 0)}")
    summary_lines.append(f"production_rows_shares_blocked={stats.get('rows_shares_blocked', 0)}")
    if "missing_columns" in stats:
        summary_lines.append(f"production_missing_columns={stats['missing_columns']}")

    if plan_df.empty:
        summary_lines.append("production_ready=false")
        summary_lines.append("production_selected_trades=0")
        if args.prod_fail_if_empty:
            summary_lines.append("production_reason=empty_plan_and_fail_if_empty")
            return 1
        summary_lines.append("production_reason=empty_plan")
        return 0

    total_k = float(plan_df["kalshi_notional_usd"].sum())
    total_p = float(plan_df["poly_notional_usd"].sum())
    total_k_spent = float((plan_df["kalshi_notional_usd"] + plan_df["kalshi_fee_usd"]).sum())
    total_p_spent = float((plan_df["poly_notional_usd"] + plan_df["poly_fee_usd"]).sum())
    total_cost = float(plan_df["total_cost_usd"].sum())
    total_payout = float(plan_df["payout_usd_if_hedged"].sum())
    total_expected_profit = float(plan_df["expected_profit_usd"].sum())
    avg_expected_roi = float(plan_df["expected_roi_pct"].mean())

    summary_lines.append("production_ready=true")
    summary_lines.append(f"production_selected_trades={len(plan_df)}")
    summary_lines.append(f"production_alloc_kalshi_usd={total_k:.6f}")
    summary_lines.append(f"production_alloc_poly_usd={total_p:.6f}")
    summary_lines.append(f"production_spent_kalshi_with_fee_usd={total_k_spent:.6f}")
    summary_lines.append(f"production_spent_poly_with_fee_usd={total_p_spent:.6f}")
    summary_lines.append(f"production_total_cost_usd={total_cost:.6f}")
    summary_lines.append(f"production_total_payout_usd={total_payout:.6f}")
    summary_lines.append(f"production_total_expected_profit_usd={total_expected_profit:.6f}")
    summary_lines.append(f"production_avg_expected_roi_pct={avg_expected_roi:.6f}")
    summary_lines.append(f"production_budget_remaining_kalshi_usd={max(0.0, args.prod_max_usd_kalshi - total_k_spent):.6f}")
    summary_lines.append(f"production_budget_remaining_poly_usd={max(0.0, args.prod_max_usd_poly - total_p_spent):.6f}")
    return 0


def run_replay(args, repo_root: Path, output_dir: Path, summary_lines: list[str]) -> int:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    opps_csv = output_dir / f"arb_opportunities_replay_{ts}.csv"
    diag_csv = output_dir / f"arb_diagnostics_replay_{ts}.csv"
    analysis_summary = output_dir / f"arb_analysis_replay_{ts}.txt"

    res = run_analyzer(
        args.python_exe,
        Path(args.analyzer),
        repo_root,
        Path(args.kalshi_file).resolve(),
        Path(args.poly_file).resolve(),
        opps_csv,
        diag_csv,
        analysis_summary,
        args.tolerance_sec,
        args.fee_poly_bps,
        args.fee_kalshi_bps,
        args.min_edge_pct,
        args.slippage_expected_bps,
        args.leg_risk_cost,
        args.payout_esperado,
    )

    summary_lines.append("[replay]")
    summary_lines.append(f"analyzer_rc={res.returncode}")
    summary_lines.append(f"opps_csv={opps_csv}")
    summary_lines.append(f"diag_csv={diag_csv}")
    if res.stdout.strip():
        summary_lines.append("analyzer_stdout:")
        summary_lines.extend([f"  {line}" for line in res.stdout.strip().splitlines()])
    if res.stderr.strip():
        summary_lines.append("analyzer_stderr:")
        summary_lines.extend([f"  {line}" for line in res.stderr.strip().splitlines()])

    if opps_csv.exists():
        opp_count = len(pd.read_csv(opps_csv))
        summary_lines.append(f"opportunities_rows={opp_count}")
    if diag_csv.exists():
        diag_df = pd.read_csv(diag_csv)
        summary_lines.append(f"diagnostics_rows={len(diag_df)}")
        if "error_code" in diag_df.columns and len(diag_df) > 0:
            summary_lines.append("diagnostics_by_error_code:")
            for code, count in diag_df["error_code"].fillna("unknown").value_counts().items():
                summary_lines.append(f"  - {code}: {int(count)}")

    return 0 if res.returncode == 0 else 1


def run_live_observe(args, repo_root: Path, output_dir: Path, summary_lines: list[str]) -> int:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    k_csv = output_dir / f"kalshi_liveobserve_{ts}.csv"
    p_csv = output_dir / f"poly_liveobserve_{ts}.csv"
    opps_csv = output_dir / f"arb_opportunities_liveobserve_{ts}.csv"
    diag_csv = output_dir / f"arb_diagnostics_liveobserve_{ts}.csv"
    analysis_summary = output_dir / f"arb_analysis_liveobserve_{ts}.txt"

    cmd_k = [
        args.python_exe,
        str(Path(args.watch_kalshi)),
        "--interval",
        str(args.live_interval_sec),
        "--max-seconds",
        str(args.live_duration_sec),
        "--csv-file",
        str(k_csv),
    ]
    cmd_p = [
        args.python_exe,
        str(Path(args.watch_poly)),
        "--interval",
        str(args.live_interval_sec),
        "--max-seconds",
        str(args.live_duration_sec),
        "--csv-file",
        str(p_csv),
    ]

    summary_lines.append("[live-observe]")
    summary_lines.append(f"kalshi_cmd={' '.join(cmd_k)}")
    summary_lines.append(f"poly_cmd={' '.join(cmd_p)}")

    p_k = subprocess.Popen(cmd_k, cwd=str(repo_root), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    p_p = subprocess.Popen(cmd_p, cwd=str(repo_root), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    timeout = int(args.live_duration_sec) + 120
    live_ok = True
    try:
        out_k, _ = p_k.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        p_k.kill()
        out_k, _ = p_k.communicate()
        live_ok = False
        summary_lines.append("kalshi_timeout=true")
    try:
        out_p, _ = p_p.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        p_p.kill()
        out_p, _ = p_p.communicate()
        live_ok = False
        summary_lines.append("poly_timeout=true")

    summary_lines.append(f"kalshi_rc={p_k.returncode}")
    summary_lines.append(f"poly_rc={p_p.returncode}")
    if out_k.strip():
        summary_lines.append("kalshi_output_tail:")
        summary_lines.extend([f"  {line}" for line in out_k.strip().splitlines()[-8:]])
    if out_p.strip():
        summary_lines.append("poly_output_tail:")
        summary_lines.extend([f"  {line}" for line in out_p.strip().splitlines()[-8:]])

    analyzer_res = run_analyzer(
        args.python_exe,
        Path(args.analyzer),
        repo_root,
        k_csv,
        p_csv,
        opps_csv,
        diag_csv,
        analysis_summary,
        args.tolerance_sec,
        args.fee_poly_bps,
        args.fee_kalshi_bps,
        args.min_edge_pct,
        args.slippage_expected_bps,
        args.leg_risk_cost,
        args.payout_esperado,
    )
    summary_lines.append(f"analyzer_rc={analyzer_res.returncode}")
    summary_lines.append(f"opps_csv={opps_csv}")
    summary_lines.append(f"diag_csv={diag_csv}")

    if analyzer_res.stdout.strip():
        summary_lines.append("analyzer_stdout:")
        summary_lines.extend([f"  {line}" for line in analyzer_res.stdout.strip().splitlines()])
    if analyzer_res.stderr.strip():
        summary_lines.append("analyzer_stderr:")
        summary_lines.extend([f"  {line}" for line in analyzer_res.stderr.strip().splitlines()])

    return 0 if (live_ok and p_k.returncode == 0 and p_p.returncode == 0 and analyzer_res.returncode == 0) else 1


def run_fault_injection(args, repo_root: Path, output_dir: Path, summary_lines: list[str]) -> int:
    base_k = pd.read_csv(Path(args.kalshi_file).resolve())
    base_p = pd.read_csv(Path(args.poly_file).resolve())

    scenarios = [
        ("schema_drift", scenario_schema_drift),
        ("type_mismatch", scenario_type_mismatch),
        ("ws_disconnect_timeout", scenario_ws_disconnect_timeout),
        ("market_rollover_race", scenario_market_rollover_race),
        ("stale_quotes", scenario_stale_quotes),
        ("join_inflation", scenario_join_inflation),
        ("missing_book_side", scenario_missing_book_side),
    ]

    fault_dir = output_dir / "fault_injection"
    fault_dir.mkdir(parents=True, exist_ok=True)

    summary_lines.append("[fault-injection]")
    any_fail = False

    for name, scenario_fn in scenarios:
        s_k, s_p = scenario_fn(base_k, base_p)
        k_path = fault_dir / f"{name}_kalshi.csv"
        p_path = fault_dir / f"{name}_poly.csv"
        opps_path = fault_dir / f"{name}_opps.csv"
        diag_path = fault_dir / f"{name}_diag.csv"
        analysis_summary = fault_dir / f"{name}_analysis.txt"

        s_k.to_csv(k_path, index=False)
        s_p.to_csv(p_path, index=False)

        res = run_analyzer(
            args.python_exe,
            Path(args.analyzer),
            repo_root,
            k_path,
            p_path,
            opps_path,
            diag_path,
            analysis_summary,
            args.tolerance_sec,
            args.fee_poly_bps,
            args.fee_kalshi_bps,
            args.min_edge_pct,
            args.slippage_expected_bps,
            args.leg_risk_cost,
            args.payout_esperado,
        )

        summary_lines.append(f"scenario={name} rc={res.returncode}")
        if res.returncode != 0:
            any_fail = True
            if res.stderr.strip():
                summary_lines.append(f"  stderr={res.stderr.strip()}")
            continue

        opp_rows = 0
        diag_rows = 0
        if opps_path.exists():
            opp_rows = len(pd.read_csv(opps_path))
        if diag_path.exists():
            diag_df = pd.read_csv(diag_path)
            diag_rows = len(diag_df)
            top_codes = (
                diag_df["error_code"].fillna("unknown").value_counts().head(5).to_dict()
                if "error_code" in diag_df.columns
                else {}
            )
        else:
            top_codes = {}

        summary_lines.append(f"  opp_rows={opp_rows} diag_rows={diag_rows}")
        if top_codes:
            summary_lines.append("  top_error_codes:")
            for code, count in top_codes.items():
                summary_lines.append(f"    - {code}: {int(count)}")

    return 1 if any_fail else 0


def main():
    args = parse_args()
    warnings.warn("DEPRECATED: use scripts/crypto_cli.py", UserWarning, stacklevel=1)
    print("DEPRECATED: use scripts/crypto_cli.py", file=sys.stderr)

    cfg_errors: list[str] = []
    if _validate_from_arb_cli is not None:
        try:
            _, cfg_errors = _validate_from_arb_cli(args)
        except Exception as e:
            cfg_errors = [f"arb_cli_validation_error={e}"]

    args.kalshi_file = str(Path(args.kalshi_file).resolve())
    args.poly_file = str(Path(args.poly_file).resolve())
    args.analyzer = str(Path(args.analyzer).resolve())
    args.watch_kalshi = str(Path(args.watch_kalshi).resolve())
    args.watch_poly = str(Path(args.watch_poly).resolve())

    repo_root = Path(args.analyzer).resolve().parent.parent
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    summary_lines = []
    summary_lines.append(f"dry_run_mode={args.mode}")
    summary_lines.append(f"run_at_utc={datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}")
    summary_lines.append(f"repo_root={repo_root}")
    summary_lines.append(f"execution_mode={args.execution_mode}")
    summary_lines.append(f"min_liquidity={args.min_liquidity}")
    summary_lines.append(f"slippage_expected_bps={args.slippage_expected_bps}")
    summary_lines.append(f"leg_risk_cost={args.leg_risk_cost}")
    summary_lines.append(f"payout_esperado={args.payout_esperado}")
    summary_lines.append(f"max_losses_streak={args.max_losses_streak}")
    summary_lines.append(f"max_daily_drawdown_pct={args.max_daily_drawdown_pct}")
    summary_lines.append(f"max_open_positions={args.max_open_positions}")
    summary_lines.append(f"kill_switch_path={args.kill_switch_path}")
    summary_lines.append(f"leg_timeout_sec={args.leg_timeout_sec}")
    summary_lines.append(f"hedge_timeout_sec={args.hedge_timeout_sec}")
    summary_lines.append(f"exec_sim_partial_fill_ratio={args.exec_sim_partial_fill_ratio}")
    summary_lines.append(f"exec_sim_leg_latency_sec={args.exec_sim_leg_latency_sec}")
    summary_lines.append(f"exec_force_hedge_fail={args.exec_force_hedge_fail}")
    summary_lines.append("legacy_entrypoint_deprecated=true")
    summary_lines.append("legacy_replacement=scripts/crypto_cli.py")
    if _validate_from_arb_cli is None:
        summary_lines.append("arb_cli_validation=skipped_import_error")
    else:
        summary_lines.append(f"arb_cli_validation_errors={len(cfg_errors)}")
        for err in cfg_errors:
            summary_lines.append(f"arb_cli_validation_error={err}")
    if cfg_errors:
        rc = 1
        summary_lines.append("exit_code=1")
        summary_text = "\n".join(summary_lines) + "\n"
        Path(args.summary_file).resolve().write_text(summary_text, encoding="utf-8")
        print(summary_text, end="")
        return

    rc = 1
    try:
        if args.mode == "replay":
            rc = run_replay(args, repo_root, output_dir, summary_lines)
        elif args.mode == "live-sim":
            rc = run_live_observe(args, repo_root, output_dir, summary_lines)
        elif args.mode == "live-observe":
            rc = run_live_observe(args, repo_root, output_dir, summary_lines)
        elif args.mode in {"live-shadow", "live-prod"}:
            rc = run_live_mode(args, repo_root, output_dir, summary_lines)
        elif args.mode == "fault-injection":
            rc = run_fault_injection(args, repo_root, output_dir, summary_lines)
        elif args.mode == "pessimistic-exec":
            rc = run_pessimistic_exec(args, repo_root, output_dir, summary_lines)
        elif args.mode == "production-ready":
            rc = run_production_ready(args, repo_root, output_dir, summary_lines)
        else:
            summary_lines.append(f"unsupported_mode={args.mode}")
            rc = 1
    except Exception as e:
        summary_lines.append(f"unhandled_exception={e}")
        rc = 1

    summary_lines.append(f"exit_code={rc}")
    summary_path = Path(args.summary_file).resolve()
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    print(summary_path.read_text(encoding="utf-8"), end="")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
