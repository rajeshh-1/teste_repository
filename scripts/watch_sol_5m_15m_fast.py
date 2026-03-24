"""
Fast SOL Up/Down watcher (5m + 15m) for Polymarket.

This script polls SOL 5m and 15m in one loop and writes:
1) aggregate CSV ticks
2) raw JSONL files (trades/prices/orderbook) under data/raw/sol5m and data/raw/sol15m
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


GAMMA_HOST = "https://gamma-api.polymarket.com"
CLOB_HOST = "https://clob.polymarket.com"
DATA_API_HOST = "https://data-api.polymarket.com"
SCHEMA_VERSION = "1.1"

CSV_FIELDS = [
    "timestamp_utc",
    "schema_version",
    "row_status",
    "error_code",
    "coin",
    "timeframe",
    "slug",
    "market_key",
    "market_close_utc",
    "condition_id",
    "question",
    "label_up",
    "label_down",
    "up_mid",
    "down_mid",
    "mid_sum",
    "up_best_bid",
    "up_best_ask",
    "down_best_bid",
    "down_best_ask",
    "up_bid_size_1",
    "up_ask_size_1",
    "down_bid_size_1",
    "down_ask_size_1",
    "yes_ask_plus_no_ask",
    "yes_bid_plus_no_bid",
    "new_trades_count",
]


@dataclass
class MarketState:
    timeframe: str
    bucket_min: int
    slug: str = ""
    question: str = ""
    label_up: str = "Up"
    label_down: str = "Down"
    condition_id: str = ""
    token_up: str = ""
    token_down: str = ""
    market_key: str = ""
    market_close_utc: str = ""
    seen_trade_ids: set[str] = field(default_factory=set)


def build_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=2,
        backoff_factor=0.15,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def parse_bool(raw: str) -> bool:
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"invalid boolean value: {raw}")


def parse_json_field(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return []
    return value or []


def safe_float(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc(raw: Any) -> datetime | None:
    text = str(raw or "").strip()
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


def floor_to_bucket(now_utc: datetime, bucket_min: int) -> datetime:
    minute = (now_utc.minute // bucket_min) * bucket_min
    return now_utc.replace(minute=minute, second=0, microsecond=0)


def build_slug(coin: str, bucket_min: int) -> str:
    now_utc = datetime.now(timezone.utc)
    start_utc = floor_to_bucket(now_utc, bucket_min)
    tf = f"{bucket_min}m"
    return f"{coin}-updown-{tf}-{int(start_utc.timestamp())}"


def market_close_utc_from_slug(slug: str, bucket_min: int) -> str:
    try:
        start_ts = int(str(slug).rsplit("-", 1)[-1])
    except Exception:
        return ""
    dt = datetime.fromtimestamp(start_ts, tz=timezone.utc) + timedelta(minutes=bucket_min)
    return iso_utc(dt)


def market_key_from_close(coin: str, bucket_min: int, market_close_utc: str) -> str:
    if not market_close_utc:
        return ""
    return f"{coin.upper()}{bucket_min}M_{market_close_utc}"


def fetch_market_info(session: requests.Session, slug: str) -> dict | None:
    response = session.get(f"{GAMMA_HOST}/events", params={"slug": slug}, timeout=8)
    response.raise_for_status()
    events = response.json()
    if not events:
        return None

    event = events[0]
    market = None
    for item in event.get("markets", []):
        if "up or down" in str(item.get("question", "")).lower():
            market = item
            break
    if market is None:
        markets = event.get("markets", [])
        market = markets[0] if markets else None
    if market is None:
        return None

    outcomes = [str(x) for x in parse_json_field(market.get("outcomes", []))]
    token_ids = [str(x) for x in parse_json_field(market.get("clobTokenIds", []))]
    if len(outcomes) < 2 or len(token_ids) < 2:
        return None

    up_idx = next((idx for idx, out in enumerate(outcomes) if out.lower() in ("up", "yes")), 0)
    down_idx = next((idx for idx, out in enumerate(outcomes) if out.lower() in ("down", "no")), 1)

    return {
        "question": str(market.get("question", "")),
        "label_up": outcomes[up_idx],
        "label_down": outcomes[down_idx],
        "condition_id": str(market.get("conditionId", "")),
        "token_up": token_ids[up_idx],
        "token_down": token_ids[down_idx],
    }


def fetch_midpoint(session: requests.Session, token_id: str):
    response = session.get(f"{CLOB_HOST}/midpoint", params={"token_id": token_id}, timeout=5)
    response.raise_for_status()
    return safe_float(response.json().get("mid"))


def fetch_book(session: requests.Session, token_id: str) -> dict | None:
    response = session.get(f"{CLOB_HOST}/book", params={"token_id": token_id}, timeout=5)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        return None
    return payload


def fetch_recent_trades(session: requests.Session, condition_id: str, limit: int) -> list[dict]:
    response = session.get(
        f"{DATA_API_HOST}/trades",
        params={"market": condition_id, "limit": int(limit), "offset": 0},
        timeout=8,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        return []
    return payload


def rotate_csv_if_needed(path: str, fields: list[str]) -> None:
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        return
    with open(path, "r", encoding="utf-8") as fh:
        header = fh.readline().strip().split(",")
    if header == fields:
        return
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    legacy_path = f"{path}.legacy_{ts}.csv"
    os.replace(path, legacy_path)
    print(f"[CSV] schema changed, rotated legacy file: {legacy_path}")


def ensure_csv(path: str, fields: list[str]) -> None:
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)
    rotate_csv_if_needed(path, fields)
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()


def append_csv(path: str, fields: list[str], row: dict) -> None:
    with open(path, "a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writerow(row)


def append_jsonl(path: str, row: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(row, ensure_ascii=True) + "\n")


def normalize_book_levels(levels: Any, side: str, depth: int) -> list[dict[str, float]]:
    if not isinstance(levels, list):
        return []
    rows: list[dict[str, float]] = []
    for item in levels:
        if not isinstance(item, dict):
            continue
        price = safe_float(item.get("price"))
        size = safe_float(item.get("size"))
        if price is None or size is None:
            continue
        rows.append({"price": float(price), "size": float(size)})
    reverse = side == "bid"
    rows.sort(key=lambda x: x["price"], reverse=reverse)
    if depth > 0:
        rows = rows[:depth]
    return rows


def best_price_size(levels: list[dict[str, float]]) -> tuple[float | None, float | None]:
    if not levels:
        return None, None
    top = levels[0]
    return top["price"], top["size"]


def normalize_trade_row(raw: dict[str, Any], market_key: str, token_up: str, token_down: str) -> dict[str, Any] | None:
    ts_raw = raw.get("timestamp")
    ts_dt = None
    if isinstance(ts_raw, (int, float)):
        ts_dt = datetime.fromtimestamp(float(ts_raw), tz=timezone.utc)
    elif isinstance(ts_raw, str):
        ts_dt = parse_utc(ts_raw)
    if ts_dt is None:
        return None

    price = safe_float(raw.get("price"))
    size = safe_float(raw.get("size"))
    side = str(raw.get("side", "")).strip().lower()
    tx = str(raw.get("transactionHash", "")).strip()
    asset = str(raw.get("asset", "")).strip()
    trade_id = f"{tx}:{asset}:{raw.get('timestamp', '')}"
    if price is None or size is None or side == "" or trade_id == "":
        return None
    if not (0.0 <= price <= 1.0):
        return None
    outcome = ""
    if asset == token_up:
        outcome = "up"
    elif asset == token_down:
        outcome = "down"
    return {
        "timestamp_utc": iso_utc(ts_dt),
        "market_key": market_key,
        "price": float(price),
        "size": float(size),
        "side": side,
        "trade_id": trade_id,
        "outcome": outcome,
    }


def raw_paths(raw_dir: str, coin: str, state: MarketState, timestamp_utc: str) -> tuple[str, str, str]:
    day = timestamp_utc[:10]
    folder = os.path.join(raw_dir, f"{coin.lower()}{state.bucket_min}m")
    return (
        os.path.join(folder, f"trades_{day}.jsonl"),
        os.path.join(folder, f"prices_{day}.jsonl"),
        os.path.join(folder, f"orderbook_{day}.jsonl"),
    )


def watch_once(
    *,
    session: requests.Session,
    coin: str,
    state: MarketState,
    csv_file: str,
    raw_dir: str,
    collect_orderbook: bool,
    collect_trades: bool,
    trade_limit: int,
    book_depth: int,
) -> None:
    now_dt = datetime.now(timezone.utc)
    now_iso = iso_utc(now_dt)
    error_codes: list[str] = []

    slug = build_slug(coin=coin, bucket_min=state.bucket_min)
    if slug != state.slug:
        state.slug = slug
        try:
            info = fetch_market_info(session, slug)
        except Exception as exc:
            info = None
            error_codes.append("market_lookup_error")
            print(f"{now_iso} | {coin}/{state.timeframe} | market_lookup_error={exc}")
        if info is None:
            error_codes.append("market_not_found")
            append_csv(
                csv_file,
                CSV_FIELDS,
                {
                    "timestamp_utc": now_iso,
                    "schema_version": SCHEMA_VERSION,
                    "row_status": "invalid",
                    "error_code": "|".join(error_codes),
                    "coin": coin.upper(),
                    "timeframe": state.timeframe,
                    "slug": slug,
                    "market_key": "",
                    "market_close_utc": "",
                    "condition_id": "",
                    "question": "",
                    "label_up": "",
                    "label_down": "",
                    "up_mid": "",
                    "down_mid": "",
                    "mid_sum": "",
                    "up_best_bid": "",
                    "up_best_ask": "",
                    "down_best_bid": "",
                    "down_best_ask": "",
                    "up_bid_size_1": "",
                    "up_ask_size_1": "",
                    "down_bid_size_1": "",
                    "down_ask_size_1": "",
                    "yes_ask_plus_no_ask": "",
                    "yes_bid_plus_no_bid": "",
                    "new_trades_count": 0,
                },
            )
            print(f"{now_iso} | {coin}/{state.timeframe} | market_not_found slug={slug}")
            return

        state.question = info["question"]
        state.label_up = info["label_up"]
        state.label_down = info["label_down"]
        state.condition_id = info["condition_id"]
        state.token_up = info["token_up"]
        state.token_down = info["token_down"]
        state.market_close_utc = market_close_utc_from_slug(slug=slug, bucket_min=state.bucket_min)
        state.market_key = market_key_from_close(coin=coin, bucket_min=state.bucket_min, market_close_utc=state.market_close_utc)

    up_mid = None
    down_mid = None
    try:
        up_mid = fetch_midpoint(session, state.token_up)
    except Exception:
        error_codes.append("up_mid_fetch_error")
    try:
        down_mid = fetch_midpoint(session, state.token_down)
    except Exception:
        error_codes.append("down_mid_fetch_error")

    up_book = None
    down_book = None
    if collect_orderbook:
        try:
            up_book = fetch_book(session, state.token_up)
        except Exception:
            error_codes.append("up_book_fetch_error")
        try:
            down_book = fetch_book(session, state.token_down)
        except Exception:
            error_codes.append("down_book_fetch_error")

    up_bids = normalize_book_levels(up_book.get("bids", []) if isinstance(up_book, dict) else [], "bid", book_depth)
    up_asks = normalize_book_levels(up_book.get("asks", []) if isinstance(up_book, dict) else [], "ask", book_depth)
    down_bids = normalize_book_levels(down_book.get("bids", []) if isinstance(down_book, dict) else [], "bid", book_depth)
    down_asks = normalize_book_levels(down_book.get("asks", []) if isinstance(down_book, dict) else [], "ask", book_depth)

    up_best_bid, up_bid_size_1 = best_price_size(up_bids)
    up_best_ask, up_ask_size_1 = best_price_size(up_asks)
    down_best_bid, down_bid_size_1 = best_price_size(down_bids)
    down_best_ask, down_ask_size_1 = best_price_size(down_asks)

    if collect_orderbook and (up_best_bid is None or up_best_ask is None or down_best_bid is None or down_best_ask is None):
        error_codes.append("missing_book_side")
    if up_mid is None or down_mid is None:
        error_codes.append("missing_midpoint")

    mid_sum = (up_mid + down_mid) if (up_mid is not None and down_mid is not None) else None
    yes_ask_plus_no_ask = (up_best_ask + down_best_ask) if (up_best_ask is not None and down_best_ask is not None) else None
    yes_bid_plus_no_bid = (up_best_bid + down_best_bid) if (up_best_bid is not None and down_best_bid is not None) else None
    status = "invalid" if error_codes else "valid"
    error_code = "|".join(sorted(set(error_codes)))

    trades_path, prices_path, orderbook_path = raw_paths(raw_dir=raw_dir, coin=coin, state=state, timestamp_utc=now_iso)
    new_trades_count = 0
    if collect_trades and state.condition_id:
        try:
            recent_trades = fetch_recent_trades(session, state.condition_id, trade_limit)
        except Exception:
            recent_trades = []
            error_codes.append("trades_fetch_error")
        for raw_trade in reversed(recent_trades):
            if not isinstance(raw_trade, dict):
                continue
            normalized = normalize_trade_row(raw_trade, state.market_key, state.token_up, state.token_down)
            if normalized is None:
                continue
            trade_id = normalized["trade_id"]
            if trade_id in state.seen_trade_ids:
                continue
            state.seen_trade_ids.add(trade_id)
            append_jsonl(trades_path, normalized)
            new_trades_count += 1

    price_row = {
        "timestamp_utc": now_iso,
        "market_key": state.market_key,
        "best_bid": "" if up_best_bid is None else up_best_bid,
        "best_ask": "" if up_best_ask is None else up_best_ask,
        "mid": "" if up_mid is None else up_mid,
        "yes_bid": "" if up_best_bid is None else up_best_bid,
        "yes_ask": "" if up_best_ask is None else up_best_ask,
        "no_bid": "" if down_best_bid is None else down_best_bid,
        "no_ask": "" if down_best_ask is None else down_best_ask,
        "up_mid": "" if up_mid is None else up_mid,
        "down_mid": "" if down_mid is None else down_mid,
        "mid_sum": "" if mid_sum is None else mid_sum,
        "condition_id": state.condition_id,
    }
    append_jsonl(prices_path, price_row)

    if collect_orderbook:
        append_jsonl(
            orderbook_path,
            {
                "timestamp_utc": now_iso,
                "market_key": state.market_key,
                "bids": up_bids,
                "asks": up_asks,
                "snapshot_id": f"{state.market_key}:{now_iso}",
                "yes_bids": up_bids,
                "yes_asks": up_asks,
                "no_bids": down_bids,
                "no_asks": down_asks,
            },
        )

    append_csv(
        csv_file,
        CSV_FIELDS,
        {
            "timestamp_utc": now_iso,
            "schema_version": SCHEMA_VERSION,
            "row_status": status,
            "error_code": error_code,
            "coin": coin.upper(),
            "timeframe": state.timeframe,
            "slug": state.slug,
            "market_key": state.market_key,
            "market_close_utc": state.market_close_utc,
            "condition_id": state.condition_id,
            "question": state.question,
            "label_up": state.label_up,
            "label_down": state.label_down,
            "up_mid": "" if up_mid is None else round(up_mid, 8),
            "down_mid": "" if down_mid is None else round(down_mid, 8),
            "mid_sum": "" if mid_sum is None else round(mid_sum, 8),
            "up_best_bid": "" if up_best_bid is None else round(up_best_bid, 8),
            "up_best_ask": "" if up_best_ask is None else round(up_best_ask, 8),
            "down_best_bid": "" if down_best_bid is None else round(down_best_bid, 8),
            "down_best_ask": "" if down_best_ask is None else round(down_best_ask, 8),
            "up_bid_size_1": "" if up_bid_size_1 is None else round(up_bid_size_1, 8),
            "up_ask_size_1": "" if up_ask_size_1 is None else round(up_ask_size_1, 8),
            "down_bid_size_1": "" if down_bid_size_1 is None else round(down_bid_size_1, 8),
            "down_ask_size_1": "" if down_ask_size_1 is None else round(down_ask_size_1, 8),
            "yes_ask_plus_no_ask": "" if yes_ask_plus_no_ask is None else round(yes_ask_plus_no_ask, 8),
            "yes_bid_plus_no_bid": "" if yes_bid_plus_no_bid is None else round(yes_bid_plus_no_bid, 8),
            "new_trades_count": int(new_trades_count),
        },
    )

    up_txt = f"{up_mid:.4f}" if up_mid is not None else "N/A"
    down_txt = f"{down_mid:.4f}" if down_mid is not None else "N/A"
    ask_txt = f"{yes_ask_plus_no_ask:.4f}" if yes_ask_plus_no_ask is not None else "N/A"
    print(
        f"{now_iso} | {coin}/{state.timeframe} | status={status} err={error_code or '-'} "
        f"| up={up_txt} down={down_txt} yes_ask+no_ask={ask_txt} new_trades={new_trades_count}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Watch SOL 5m and 15m prices, orderbook and trades every 50ms (best-effort).")
    parser.add_argument("--coin", default="sol", help="Coin slug prefix used in market slug (default: sol).")
    parser.add_argument("--interval", type=float, default=0.05, help="Polling interval in seconds (default: 0.05).")
    parser.add_argument("--max-seconds", type=int, default=0, help="Max runtime in seconds (0 = infinite).")
    parser.add_argument("--csv-file", default="logs/sol_5m_15m_ticks.csv", help="Aggregate output CSV file.")
    parser.add_argument("--raw-dir", default="data/raw", help="Raw output directory for trades/prices/orderbook JSONL.")
    parser.add_argument("--collect-orderbook", choices=["true", "false"], default="true")
    parser.add_argument("--collect-trades", choices=["true", "false"], default="true")
    parser.add_argument("--book-depth", type=int, default=15, help="Orderbook levels to persist per side.")
    parser.add_argument("--trade-limit", type=int, default=200, help="Recent trades pulled per cycle.")
    args = parser.parse_args()

    interval = max(0.05, float(args.interval))
    collect_orderbook = parse_bool(args.collect_orderbook)
    collect_trades = parse_bool(args.collect_trades)
    ensure_csv(args.csv_file, CSV_FIELDS)
    session = build_session()

    state_5m = MarketState(timeframe="5m", bucket_min=5)
    state_15m = MarketState(timeframe="15m", bucket_min=15)

    print("=" * 100)
    print(
        " SOL 5m+15m FULL WATCHER"
        f" | interval={interval:.3f}s | csv={args.csv_file} | raw_dir={args.raw_dir}"
        f" | orderbook={collect_orderbook} | trades={collect_trades}"
    )
    print("=" * 100)

    started = time.time()
    while True:
        cycle_start = time.time()
        if args.max_seconds > 0 and (cycle_start - started) >= args.max_seconds:
            print("[WATCHER] finished by --max-seconds.")
            break

        try:
            watch_once(
                session=session,
                coin=str(args.coin).strip().lower(),
                state=state_5m,
                csv_file=args.csv_file,
                raw_dir=str(args.raw_dir),
                collect_orderbook=collect_orderbook,
                collect_trades=collect_trades,
                trade_limit=max(1, int(args.trade_limit)),
                book_depth=max(1, int(args.book_depth)),
            )
        except Exception as exc:
            print(f"{iso_utc(datetime.now(timezone.utc))} | {args.coin}/5m | error={exc}")

        try:
            watch_once(
                session=session,
                coin=str(args.coin).strip().lower(),
                state=state_15m,
                csv_file=args.csv_file,
                raw_dir=str(args.raw_dir),
                collect_orderbook=collect_orderbook,
                collect_trades=collect_trades,
                trade_limit=max(1, int(args.trade_limit)),
                book_depth=max(1, int(args.book_depth)),
            )
        except Exception as exc:
            print(f"{iso_utc(datetime.now(timezone.utc))} | {args.coin}/15m | error={exc}")

        elapsed = time.time() - cycle_start
        sleep_for = interval - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)


if __name__ == "__main__":
    main()
