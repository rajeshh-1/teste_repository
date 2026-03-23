"""
Fast SOL Up/Down watcher (5m + 15m) for Polymarket.

This script polls both SOL 5m and SOL 15m markets in a single loop and writes
rows to one CSV. Interval is best-effort and defaults to 50ms.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


GAMMA_HOST = "https://gamma-api.polymarket.com"
CLOB_HOST = "https://clob.polymarket.com"
SCHEMA_VERSION = "1.0"

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
    "question",
    "label_up",
    "label_down",
    "up_mid",
    "down_mid",
    "mid_sum",
]


@dataclass
class MarketState:
    timeframe: str
    bucket_min: int
    slug: str = ""
    question: str = ""
    label_up: str = "Up"
    label_down: str = "Down"
    token_up: str = ""
    token_down: str = ""
    market_key: str = ""
    market_close_utc: str = ""


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
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


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
        "token_up": token_ids[up_idx],
        "token_down": token_ids[down_idx],
    }


def fetch_midpoint(session: requests.Session, token_id: str):
    response = session.get(f"{CLOB_HOST}/midpoint", params={"token_id": token_id}, timeout=5)
    response.raise_for_status()
    return safe_float(response.json().get("mid"))


def ensure_csv(path: str, fields: list[str]) -> None:
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()


def append_csv(path: str, fields: list[str], row: dict) -> None:
    with open(path, "a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writerow(row)


def watch_once(session: requests.Session, coin: str, state: MarketState, csv_file: str) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    error_codes: list[str] = []

    slug = build_slug(coin=coin, bucket_min=state.bucket_min)
    if slug != state.slug:
        state.slug = slug
        info = fetch_market_info(session, slug)
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
                    "question": "",
                    "label_up": "",
                    "label_down": "",
                    "up_mid": "",
                    "down_mid": "",
                    "mid_sum": "",
                },
            )
            print(f"{now_iso} | {coin}/{state.timeframe} | market_not_found slug={slug}")
            return

        state.question = info["question"]
        state.label_up = info["label_up"]
        state.label_down = info["label_down"]
        state.token_up = info["token_up"]
        state.token_down = info["token_down"]
        state.market_close_utc = market_close_utc_from_slug(slug=slug, bucket_min=state.bucket_min)
        state.market_key = market_key_from_close(coin=coin, bucket_min=state.bucket_min, market_close_utc=state.market_close_utc)

    up_mid = fetch_midpoint(session, state.token_up)
    down_mid = fetch_midpoint(session, state.token_down)

    if up_mid is None or down_mid is None:
        error_codes.append("missing_book_side")

    mid_sum = (up_mid + down_mid) if (up_mid is not None and down_mid is not None) else None
    status = "invalid" if error_codes else "valid"
    error_code = "|".join(error_codes)

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
            "question": state.question,
            "label_up": state.label_up,
            "label_down": state.label_down,
            "up_mid": "" if up_mid is None else round(up_mid, 8),
            "down_mid": "" if down_mid is None else round(down_mid, 8),
            "mid_sum": "" if mid_sum is None else round(mid_sum, 8),
        },
    )

    up_txt = f"{up_mid:.4f}" if up_mid is not None else "N/A"
    down_txt = f"{down_mid:.4f}" if down_mid is not None else "N/A"
    sum_txt = f"{mid_sum:.4f}" if mid_sum is not None else "N/A"
    print(f"{now_iso} | {coin}/{state.timeframe} | status={status} err={error_code or '-'} | up={up_txt} down={down_txt} sum={sum_txt}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Watch SOL 5m and 15m prices every 50ms (best-effort).")
    parser.add_argument("--coin", default="sol", help="Coin slug prefix used in market slug (default: sol).")
    parser.add_argument("--interval", type=float, default=0.05, help="Polling interval in seconds (default: 0.05).")
    parser.add_argument("--max-seconds", type=int, default=0, help="Max runtime in seconds (0 = infinite).")
    parser.add_argument("--csv-file", default="logs/sol_5m_15m_ticks.csv", help="Output CSV file.")
    args = parser.parse_args()

    interval = max(0.05, float(args.interval))
    ensure_csv(args.csv_file, CSV_FIELDS)
    session = build_session()

    state_5m = MarketState(timeframe="5m", bucket_min=5)
    state_15m = MarketState(timeframe="15m", bucket_min=15)

    print("=" * 88)
    print(f" SOL 5m+15m FAST WATCHER | interval={interval:.3f}s | csv={args.csv_file}")
    print("=" * 88)

    started = time.time()
    while True:
        cycle_start = time.time()
        if args.max_seconds > 0 and (cycle_start - started) >= args.max_seconds:
            print("[WATCHER] finished by --max-seconds.")
            break

        try:
            watch_once(session=session, coin=str(args.coin).strip().lower(), state=state_5m, csv_file=args.csv_file)
        except Exception as exc:
            print(f"{datetime.now(timezone.utc).isoformat()} | {args.coin}/5m | error={exc}")

        try:
            watch_once(session=session, coin=str(args.coin).strip().lower(), state=state_15m, csv_file=args.csv_file)
        except Exception as exc:
            print(f"{datetime.now(timezone.utc).isoformat()} | {args.coin}/15m | error={exc}")

        elapsed = time.time() - cycle_start
        sleep_for = interval - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)


if __name__ == "__main__":
    main()
