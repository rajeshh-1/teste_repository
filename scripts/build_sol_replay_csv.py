from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


REPLAY_COLUMNS = [
    "market_key",
    "timestamp_utc",
    "edge_liq_pct",
    "seconds_to_close",
    "leg2_latency_ms",
    "partial_fill_prob",
    "timeout_prob",
    "hedge_fail_prob",
    "unwind_loss_bps",
]

MARKET_TO_FOLDER = {
    "SOL5M": "sol5m",
    "SOL15M": "sol15m",
}

SOURCE_PRIORITY = {
    "trade": 1,
    "orderbook": 2,
    "price": 3,
}


@dataclass
class ReplayStats:
    market: str
    date_utc: str
    rows_trades_read: int = 0
    rows_prices_read: int = 0
    rows_orderbook_read: int = 0
    rows_final: int = 0
    discarded_rows: int = 0
    discard_reasons: Counter[str] = field(default_factory=Counter)
    warnings: Counter[str] = field(default_factory=Counter)

    def discard(self, reason: str) -> None:
        self.discarded_rows += 1
        self.discard_reasons[reason] += 1


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize SOL raw jsonl files into replay CSV for single-pass grid.")
    parser.add_argument("--market", choices=["SOL5M", "SOL15M", "both"], default="both")
    parser.add_argument("--date", default=None, help="UTC date (YYYY-MM-DD). Defaults to today UTC.")
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--out-dir", default="data/replay")
    parser.add_argument("--default-leg2-latency-ms", type=int, default=300)
    parser.add_argument("--default-partial-fill-prob", type=float, default=0.10)
    parser.add_argument("--default-timeout-prob", type=float, default=0.03)
    parser.add_argument("--default-hedge-fail-prob", type=float, default=0.03)
    parser.add_argument("--default-unwind-loss-bps", type=float, default=50.0)
    return parser.parse_args(argv)


def _parse_date(raw: str | None) -> date:
    if not raw:
        return datetime.now(timezone.utc).date()
    return datetime.strptime(str(raw), "%Y-%m-%d").date()


def _selected_markets(value: str) -> list[str]:
    v = str(value).strip().upper()
    if v == "BOTH":
        return ["SOL5M", "SOL15M"]
    return [v]


def _parse_utc(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_close_utc(market_key: str) -> datetime | None:
    key = str(market_key or "").strip()
    if "_" not in key:
        return None
    maybe_close = key.split("_", 1)[1]
    return _parse_utc(maybe_close)


def _seconds_to_close(timestamp_utc: str, market_key: str) -> tuple[int, bool]:
    ts = _parse_utc(timestamp_utc)
    close = _parse_close_utc(market_key)
    if ts is None or close is None:
        return 0, False
    sec = int((close - ts).total_seconds())
    if sec < 0:
        return 0, True
    return sec, False


def _valid_probability(raw: float) -> float:
    return max(0.0, min(1.0, float(raw)))


def _valid_nonneg(raw: float) -> float:
    return max(0.0, float(raw))


def _load_jsonl(path: Path, stats: ReplayStats, source: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        stats.warnings[f"{source}_missing"] += 1
        return rows
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line_txt = line.strip()
            if not line_txt:
                continue
            try:
                row = json.loads(line_txt)
            except json.JSONDecodeError:
                stats.discard(f"{source}_invalid_json")
                continue
            if not isinstance(row, dict):
                stats.discard(f"{source}_invalid_object")
                continue
            rows.append(row)
    return rows


def _best_bid_from_levels(levels: Any) -> float | None:
    if not isinstance(levels, list):
        return None
    prices = [_safe_float(item.get("price")) for item in levels if isinstance(item, dict)]
    cleaned = [p for p in prices if p is not None]
    if not cleaned:
        return None
    return max(cleaned)


def _best_ask_from_levels(levels: Any) -> float | None:
    if not isinstance(levels, list):
        return None
    prices = [_safe_float(item.get("price")) for item in levels if isinstance(item, dict)]
    cleaned = [p for p in prices if p is not None]
    if not cleaned:
        return None
    return min(cleaned)


def _derive_edge_liq_pct(yes_ask: float | None, no_ask: float | None) -> float | None:
    if yes_ask is None or no_ask is None:
        return None
    if yes_ask < 0.0 or no_ask < 0.0 or yes_ask > 1.0 or no_ask > 1.0:
        return None
    total_cost = yes_ask + no_ask
    if total_cost <= 0.0:
        return None
    edge = ((1.0 - total_cost) / total_cost) * 100.0
    return max(0.0, edge)


def _derive_edge_from_price_row(row: dict[str, Any]) -> float | None:
    explicit = _safe_float(row.get("edge_liq_pct"))
    if explicit is not None:
        return max(0.0, explicit)
    yes_ask = _safe_float(row.get("yes_ask"))
    no_ask = _safe_float(row.get("no_ask"))
    if yes_ask is None:
        yes_ask = _safe_float(row.get("best_ask"))
    if no_ask is None:
        best_bid = _safe_float(row.get("best_bid"))
        if best_bid is not None:
            no_ask = max(0.0, min(1.0, 1.0 - best_bid))
    return _derive_edge_liq_pct(yes_ask, no_ask)


def _derive_edge_from_orderbook_row(row: dict[str, Any]) -> float | None:
    explicit = _safe_float(row.get("edge_liq_pct"))
    if explicit is not None:
        return max(0.0, explicit)
    yes_ask = _best_ask_from_levels(row.get("asks"))
    best_bid = _best_bid_from_levels(row.get("bids"))
    no_ask = None if best_bid is None else max(0.0, min(1.0, 1.0 - best_bid))
    return _derive_edge_liq_pct(yes_ask, no_ask)


def _base_candidate(
    *,
    row: dict[str, Any],
    stats: ReplayStats,
    default_leg2_latency_ms: int,
    default_partial_fill_prob: float,
    default_timeout_prob: float,
    default_hedge_fail_prob: float,
    default_unwind_loss_bps: float,
    edge_liq_pct: float | None,
) -> dict[str, Any] | None:
    market_key = str(row.get("market_key", "")).strip()
    if not market_key:
        stats.discard("empty_market_key")
        return None
    timestamp_utc = str(row.get("timestamp_utc", "")).strip()
    ts = _parse_utc(timestamp_utc)
    if ts is None:
        stats.discard("invalid_timestamp_utc")
        return None
    timestamp_norm = _iso_utc(ts)
    seconds, clamped = _seconds_to_close(timestamp_norm, market_key)
    if clamped:
        stats.warnings["seconds_to_close_clamped"] += 1
    if edge_liq_pct is None:
        edge_liq_pct = 0.0
        stats.warnings["edge_fallback_zero"] += 1

    return {
        "market_key": market_key,
        "timestamp_utc": timestamp_norm,
        "edge_liq_pct": round(max(0.0, float(edge_liq_pct)), 8),
        "seconds_to_close": int(max(0, seconds)),
        "leg2_latency_ms": int(max(0, int(default_leg2_latency_ms))),
        "partial_fill_prob": round(_valid_probability(default_partial_fill_prob), 8),
        "timeout_prob": round(_valid_probability(default_timeout_prob), 8),
        "hedge_fail_prob": round(_valid_probability(default_hedge_fail_prob), 8),
        "unwind_loss_bps": round(_valid_nonneg(default_unwind_loss_bps), 8),
    }


def _upsert_row(
    dedup_rows: dict[tuple[str, str], dict[str, Any]],
    dedup_source: dict[tuple[str, str], str],
    *,
    candidate: dict[str, Any],
    source: str,
    stats: ReplayStats,
) -> None:
    key = (candidate["market_key"], candidate["timestamp_utc"])
    if key not in dedup_rows:
        dedup_rows[key] = candidate
        dedup_source[key] = source
        return
    old_source = dedup_source[key]
    if SOURCE_PRIORITY[source] > SOURCE_PRIORITY[old_source]:
        dedup_rows[key] = candidate
        dedup_source[key] = source
        stats.warnings["dedup_replaced_higher_priority"] += 1
        return
    if SOURCE_PRIORITY[source] == SOURCE_PRIORITY[old_source]:
        if float(candidate["edge_liq_pct"]) > float(dedup_rows[key]["edge_liq_pct"]):
            dedup_rows[key] = candidate
            stats.warnings["dedup_replaced_higher_edge"] += 1
            return
    stats.discard("duplicate_market_key_timestamp")


def _transform_market(
    *,
    market: str,
    target_date: date,
    raw_dir: Path,
    out_dir: Path,
    default_leg2_latency_ms: int,
    default_partial_fill_prob: float,
    default_timeout_prob: float,
    default_hedge_fail_prob: float,
    default_unwind_loss_bps: float,
) -> ReplayStats:
    day = target_date.strftime("%Y-%m-%d")
    stats = ReplayStats(market=market, date_utc=day)
    folder = MARKET_TO_FOLDER[market]
    market_raw = raw_dir / folder

    trades_path = market_raw / f"trades_{day}.jsonl"
    prices_path = market_raw / f"prices_{day}.jsonl"
    orderbook_path = market_raw / f"orderbook_{day}.jsonl"

    trades = _load_jsonl(trades_path, stats, "trades")
    prices = _load_jsonl(prices_path, stats, "prices")
    orderbooks = _load_jsonl(orderbook_path, stats, "orderbook")
    stats.rows_trades_read = len(trades)
    stats.rows_prices_read = len(prices)
    stats.rows_orderbook_read = len(orderbooks)

    dedup_rows: dict[tuple[str, str], dict[str, Any]] = {}
    dedup_source: dict[tuple[str, str], str] = {}

    for row in trades:
        candidate = _base_candidate(
            row=row,
            stats=stats,
            default_leg2_latency_ms=default_leg2_latency_ms,
            default_partial_fill_prob=default_partial_fill_prob,
            default_timeout_prob=default_timeout_prob,
            default_hedge_fail_prob=default_hedge_fail_prob,
            default_unwind_loss_bps=default_unwind_loss_bps,
            edge_liq_pct=_safe_float(row.get("edge_liq_pct")),
        )
        if candidate is None:
            continue
        _upsert_row(dedup_rows, dedup_source, candidate=candidate, source="trade", stats=stats)

    for row in orderbooks:
        candidate = _base_candidate(
            row=row,
            stats=stats,
            default_leg2_latency_ms=default_leg2_latency_ms,
            default_partial_fill_prob=default_partial_fill_prob,
            default_timeout_prob=default_timeout_prob,
            default_hedge_fail_prob=default_hedge_fail_prob,
            default_unwind_loss_bps=default_unwind_loss_bps,
            edge_liq_pct=_derive_edge_from_orderbook_row(row),
        )
        if candidate is None:
            continue
        _upsert_row(dedup_rows, dedup_source, candidate=candidate, source="orderbook", stats=stats)

    for row in prices:
        candidate = _base_candidate(
            row=row,
            stats=stats,
            default_leg2_latency_ms=default_leg2_latency_ms,
            default_partial_fill_prob=default_partial_fill_prob,
            default_timeout_prob=default_timeout_prob,
            default_hedge_fail_prob=default_hedge_fail_prob,
            default_unwind_loss_bps=default_unwind_loss_bps,
            edge_liq_pct=_derive_edge_from_price_row(row),
        )
        if candidate is None:
            continue
        _upsert_row(dedup_rows, dedup_source, candidate=candidate, source="price", stats=stats)

    final_rows = sorted(
        dedup_rows.values(),
        key=lambda x: (x["timestamp_utc"], x["market_key"]),
    )
    stats.rows_final = len(final_rows)

    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"{folder}_{day}_replay.csv"
    with output_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=REPLAY_COLUMNS)
        writer.writeheader()
        for row in final_rows:
            writer.writerow(row)

    print(f"[{market}] trades_read={stats.rows_trades_read}")
    print(f"[{market}] prices_read={stats.rows_prices_read}")
    print(f"[{market}] orderbook_read={stats.rows_orderbook_read}")
    print(f"[{market}] replay_rows_valid={stats.rows_final}")
    print(f"[{market}] discarded_rows={stats.discarded_rows}")
    if stats.discard_reasons:
        top_reason, top_count = stats.discard_reasons.most_common(1)[0]
        print(f"[{market}] discard_main_reason={top_reason} count={top_count}")
    else:
        print(f"[{market}] discard_main_reason=none")
    print(f"[{market}] output={output_path}")
    return stats


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    target_date = _parse_date(args.date)
    markets = _selected_markets(args.market)
    raw_dir = Path(args.raw_dir)
    out_dir = Path(args.out_dir)

    print(f"target_date_utc={target_date.isoformat()}")
    print(f"markets={','.join(markets)}")
    print(f"raw_dir={raw_dir.resolve()}")
    print(f"out_dir={out_dir.resolve()}")

    for market in markets:
        _transform_market(
            market=market,
            target_date=target_date,
            raw_dir=raw_dir,
            out_dir=out_dir,
            default_leg2_latency_ms=int(args.default_leg2_latency_ms),
            default_partial_fill_prob=float(args.default_partial_fill_prob),
            default_timeout_prob=float(args.default_timeout_prob),
            default_hedge_fail_prob=float(args.default_hedge_fail_prob),
            default_unwind_loss_bps=float(args.default_unwind_loss_bps),
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
