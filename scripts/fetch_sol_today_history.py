from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests


GAMMA_HOST = "https://gamma-api.polymarket.com"
CLOB_HOST = "https://clob.polymarket.com"
DATA_API_HOST = "https://data-api.polymarket.com"


@dataclass(frozen=True)
class MarketSpec:
    market: str
    coin: str
    timeframe: str
    bucket_min: int
    out_folder: str


@dataclass
class CollectStats:
    rows_trades: int = 0
    rows_prices: int = 0
    rows_orderbook: int = 0
    errors_count: int = 0
    warnings: list[str] | None = None
    orderbook_available: bool = True

    def __post_init__(self) -> None:
        if self.warnings is None:
            self.warnings = []


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_bool(raw: str) -> bool:
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"invalid boolean value: {raw}")


def _market_specs(market_arg: str) -> list[MarketSpec]:
    value = str(market_arg).strip().upper()
    mapping = {
        "SOL5M": MarketSpec(market="SOL5M", coin="sol", timeframe="5m", bucket_min=5, out_folder="sol5m"),
        "SOL15M": MarketSpec(market="SOL15M", coin="sol", timeframe="15m", bucket_min=15, out_folder="sol15m"),
    }
    if value == "BOTH":
        return [mapping["SOL5M"], mapping["SOL15M"]]
    if value in mapping:
        return [mapping[value]]
    raise ValueError("market must be SOL5M, SOL15M or both")


def _parse_date(raw: str | None) -> date:
    if not raw:
        return _utc_now().date()
    return datetime.strptime(str(raw), "%Y-%m-%d").date()


def _day_window_utc(day: date) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _build_slug(coin: str, timeframe: str, block_start_utc: datetime) -> str:
    return f"{coin}-updown-{timeframe}-{int(block_start_utc.timestamp())}"


def _market_close_utc_from_slug(slug: str, bucket_min: int) -> str:
    try:
        start_ts = int(str(slug).rsplit("-", 1)[-1])
    except Exception:
        return ""
    close_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc) + timedelta(minutes=bucket_min)
    return _iso_utc(close_dt)


def _market_key(spec: MarketSpec, market_close_utc: str) -> str:
    if not market_close_utc:
        return ""
    return f"{spec.market}_{market_close_utc}"


def _sleep_ms(value: int) -> None:
    if value > 0:
        time.sleep(float(value) / 1000.0)


def _request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None,
    timeout_sec: float,
    retries: int = 3,
) -> Any:
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.request(method, url, params=params, timeout=timeout_sec)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_exc = exc
            if attempt >= retries:
                break
            time.sleep(0.25 * attempt)
    if last_exc is None:
        raise RuntimeError("request failed without exception")
    raise last_exc


def _list_bucket_starts(spec: MarketSpec, day: date) -> list[datetime]:
    start_day, end_day = _day_window_utc(day)
    now_utc = _utc_now()
    upper = min(end_day, now_utc) if day == now_utc.date() else end_day
    out: list[datetime] = []
    cur = start_day
    while cur < upper:
        out.append(cur)
        cur += timedelta(minutes=spec.bucket_min)
    return out


def _fetch_market_info(session: requests.Session, slug: str, timeout_sec: float) -> dict[str, Any] | None:
    events = _request_json(
        session,
        "GET",
        f"{GAMMA_HOST}/events",
        params={"slug": slug},
        timeout_sec=timeout_sec,
    )
    if not isinstance(events, list) or not events:
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

    def _parse_json_field(value):
        if isinstance(value, str):
            try:
                return json.loads(value)
            except Exception:
                return []
        return value or []

    outcomes = [str(x) for x in _parse_json_field(market.get("outcomes", []))]
    token_ids = [str(x) for x in _parse_json_field(market.get("clobTokenIds", []))]
    if len(outcomes) < 2 or len(token_ids) < 2:
        return None
    up_idx = next((i for i, v in enumerate(outcomes) if v.lower() in {"up", "yes"}), 0)
    down_idx = next((i for i, v in enumerate(outcomes) if v.lower() in {"down", "no"}), 1)
    return {
        "condition_id": str(market.get("conditionId", "")),
        "question": str(market.get("question", "")),
        "token_up": token_ids[up_idx],
        "token_down": token_ids[down_idx],
    }


def _safe_float(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_trade_row(raw: dict[str, Any], market_key: str) -> dict[str, Any] | None:
    ts_raw = raw.get("timestamp")
    ts_dt: datetime | None = None
    if isinstance(ts_raw, (int, float)):
        ts_dt = datetime.fromtimestamp(float(ts_raw), tz=timezone.utc)
    elif isinstance(ts_raw, str):
        txt = ts_raw.strip().replace("Z", "+00:00")
        try:
            ts_dt = datetime.fromisoformat(txt)
            if ts_dt.tzinfo is None:
                ts_dt = ts_dt.replace(tzinfo=timezone.utc)
            ts_dt = ts_dt.astimezone(timezone.utc)
        except ValueError:
            ts_dt = None
    if ts_dt is None or not market_key:
        return None

    price = _safe_float(raw.get("price"))
    size = _safe_float(raw.get("size"))
    side = str(raw.get("side", "")).strip().lower()
    tx = str(raw.get("transactionHash", "")).strip()
    asset = str(raw.get("asset", "")).strip()
    trade_id = f"{tx}:{asset}:{raw.get('timestamp', '')}"
    if price is None or size is None or not side or not trade_id:
        return None
    if not (0.0 <= price <= 1.0):
        return None

    return {
        "timestamp_utc": _iso_utc(ts_dt),
        "market_key": market_key,
        "price": price,
        "size": size,
        "side": side,
        "trade_id": trade_id,
    }


def _fetch_trades_for_market(
    session: requests.Session,
    *,
    condition_id: str,
    market_key: str,
    day_start: datetime,
    day_end: datetime,
    page_size: int,
    max_pages: int,
    sleep_ms: int,
    timeout_sec: float,
) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    errors = 0
    seen_keys: set[str] = set()
    if not condition_id:
        return rows, errors

    for page in range(max(1, int(max_pages))):
        offset = page * int(page_size)
        try:
            data = _request_json(
                session,
                "GET",
                f"{DATA_API_HOST}/trades",
                params={"market": condition_id, "limit": int(page_size), "offset": offset},
                timeout_sec=timeout_sec,
            )
        except Exception:
            errors += 1
            break
        if not isinstance(data, list) or not data:
            break
        for item in data:
            row = _normalize_trade_row(item, market_key=market_key)
            if row is None:
                continue
            ts = datetime.fromisoformat(row["timestamp_utc"].replace("Z", "+00:00"))
            if ts < day_start or ts >= day_end:
                continue
            natural_key = f"{row['market_key']}|{row['trade_id']}"
            if natural_key in seen_keys:
                continue
            seen_keys.add(natural_key)
            rows.append(row)
        _sleep_ms(sleep_ms)
    return rows, errors


def _fetch_prices_row(
    session: requests.Session,
    *,
    token_up: str,
    token_down: str,
    market_key: str,
    timeout_sec: float,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    def _fetch_mid(token_id: str):
        return _request_json(
            session,
            "GET",
            f"{CLOB_HOST}/midpoint",
            params={"token_id": token_id},
            timeout_sec=timeout_sec,
        )

    def _fetch_book(token_id: str):
        return _request_json(
            session,
            "GET",
            f"{CLOB_HOST}/book",
            params={"token_id": token_id},
            timeout_sec=timeout_sec,
        )

    errors: list[str] = []
    now_iso = _iso_utc(_utc_now())
    try:
        up_book = _fetch_book(token_up)
        down_book = _fetch_book(token_down)
        up_mid_raw = _fetch_mid(token_up)
        down_mid_raw = _fetch_mid(token_down)
    except Exception as exc:
        return None, {"error": str(exc)}

    up_mid = _safe_float((up_mid_raw or {}).get("mid"))
    down_mid = _safe_float((down_mid_raw or {}).get("mid"))

    up_bids = up_book.get("bids", []) if isinstance(up_book, dict) else []
    down_bids = down_book.get("bids", []) if isinstance(down_book, dict) else []
    up_asks = up_book.get("asks", []) if isinstance(up_book, dict) else []
    down_asks = down_book.get("asks", []) if isinstance(down_book, dict) else []

    up_best_bid = _safe_float(up_bids[0].get("price")) if up_bids else None
    down_best_bid = _safe_float(down_bids[0].get("price")) if down_bids else None
    up_best_ask = _safe_float(up_asks[0].get("price")) if up_asks else None
    down_best_ask = _safe_float(down_asks[0].get("price")) if down_asks else None

    best_bid = up_best_bid
    best_ask = up_best_ask
    if up_best_bid is None and down_best_ask is not None:
        best_bid = 1.0 - down_best_ask
    if up_best_ask is None and down_best_bid is not None:
        best_ask = 1.0 - down_best_bid

    if best_bid is None or best_ask is None:
        errors.append("missing_best_bid_ask")

    mid = up_mid
    if mid is None and down_mid is not None:
        mid = 1.0 - down_mid
    if mid is None and best_bid is not None and best_ask is not None:
        mid = (best_bid + best_ask) / 2.0

    if mid is None:
        errors.append("missing_mid")

    if best_bid is not None and not (0.0 <= best_bid <= 1.0):
        errors.append("best_bid_out_of_range")
    if best_ask is not None and not (0.0 <= best_ask <= 1.0):
        errors.append("best_ask_out_of_range")
    if mid is not None and not (0.0 <= mid <= 1.0):
        errors.append("mid_out_of_range")

    if errors:
        return None, {"error": "|".join(errors)}

    return {
        "timestamp_utc": now_iso,
        "market_key": market_key,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid": mid,
    }, {
        "timestamp_utc": now_iso,
        "market_key": market_key,
        "bids": up_bids,
        "asks": up_asks,
        "snapshot_id": f"{market_key}:{now_iso}",
    }


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=True) + "\n")


def _collect_market(
    session: requests.Session,
    *,
    spec: MarketSpec,
    collect_date: date,
    out_dir: Path,
    include_orderbook: bool,
    page_size: int,
    max_pages: int,
    sleep_ms: int,
    timeout_sec: float,
) -> dict[str, Any]:
    started_at = _utc_now()
    day_start, day_end = _day_window_utc(collect_date)
    bucket_starts = _list_bucket_starts(spec, collect_date)
    stats = CollectStats()

    trades_rows: list[dict[str, Any]] = []
    prices_rows: list[dict[str, Any]] = []
    orderbook_rows: list[dict[str, Any]] = []
    seen_price_keys: set[str] = set()
    seen_orderbook_keys: set[str] = set()

    for idx, start_utc in enumerate(bucket_starts):
        slug = _build_slug(spec.coin, spec.timeframe, start_utc)
        close_utc = _market_close_utc_from_slug(slug, spec.bucket_min)
        market_key = _market_key(spec, close_utc)

        try:
            info = _fetch_market_info(session, slug, timeout_sec=timeout_sec)
        except Exception as exc:
            stats.errors_count += 1
            stats.warnings.append(f"{slug}:market_lookup_error:{exc}")
            _sleep_ms(sleep_ms)
            continue

        if info is None:
            stats.warnings.append(f"{slug}:market_not_found")
            _sleep_ms(sleep_ms)
            continue
        if not market_key:
            stats.errors_count += 1
            stats.warnings.append(f"{slug}:empty_market_key")
            _sleep_ms(sleep_ms)
            continue

        condition_id = str(info.get("condition_id", "")).strip()
        token_up = str(info.get("token_up", "")).strip()
        token_down = str(info.get("token_down", "")).strip()
        if not condition_id or not token_up or not token_down:
            stats.errors_count += 1
            stats.warnings.append(f"{slug}:missing_ids")
            _sleep_ms(sleep_ms)
            continue

        trades, trade_errors = _fetch_trades_for_market(
            session,
            condition_id=condition_id,
            market_key=market_key,
            day_start=day_start,
            day_end=day_end,
            page_size=page_size,
            max_pages=max_pages,
            sleep_ms=sleep_ms,
            timeout_sec=timeout_sec,
        )
        stats.errors_count += trade_errors
        trades_rows.extend(trades)

        price_row, orderbook_row = _fetch_prices_row(
            session,
            token_up=token_up,
            token_down=token_down,
            market_key=market_key,
            timeout_sec=timeout_sec,
        )
        if price_row is None:
            stats.errors_count += 1
            if isinstance(orderbook_row, dict) and orderbook_row.get("error"):
                stats.warnings.append(f"{slug}:prices_error:{orderbook_row['error']}")
        else:
            pkey = f"{price_row['market_key']}|{price_row['timestamp_utc']}"
            if pkey not in seen_price_keys:
                seen_price_keys.add(pkey)
                prices_rows.append(price_row)

        if include_orderbook:
            if orderbook_row is None or isinstance(orderbook_row, dict) and orderbook_row.get("error"):
                stats.orderbook_available = False
            elif isinstance(orderbook_row, dict):
                ob_key = f"{orderbook_row['market_key']}|{orderbook_row['snapshot_id']}"
                if ob_key not in seen_orderbook_keys:
                    seen_orderbook_keys.add(ob_key)
                    orderbook_rows.append(orderbook_row)
        else:
            stats.orderbook_available = False

        print(
            f"[{spec.market}] {idx + 1}/{len(bucket_starts)} slug={slug} "
            f"trades+={len(trades)} prices_total={len(prices_rows)} ob_total={len(orderbook_rows)}"
        )
        _sleep_ms(sleep_ms)

    stats.rows_trades = len(trades_rows)
    stats.rows_prices = len(prices_rows)
    stats.rows_orderbook = len(orderbook_rows)
    finished_at = _utc_now()

    market_dir = out_dir / spec.out_folder
    market_dir.mkdir(parents=True, exist_ok=True)
    day_str = collect_date.strftime("%Y-%m-%d")
    trades_path = market_dir / f"trades_{day_str}.jsonl"
    prices_path = market_dir / f"prices_{day_str}.jsonl"
    orderbook_path = market_dir / f"orderbook_{day_str}.jsonl"
    metadata_path = market_dir / f"metadata_{day_str}.json"

    _write_jsonl(trades_path, trades_rows)
    _write_jsonl(prices_path, prices_rows)
    if include_orderbook and orderbook_rows:
        _write_jsonl(orderbook_path, orderbook_rows)
    else:
        if orderbook_path.exists():
            orderbook_path.unlink()

    notes: list[str] = []
    if not include_orderbook:
        notes.append("orderbook collection disabled by --include-orderbook false")
    elif not stats.orderbook_available:
        notes.append("orderbook historical may be partial/unavailable for some slugs/endpoints")

    metadata = {
        "market": spec.market,
        "date_utc": day_str,
        "source": {
            "gamma": GAMMA_HOST,
            "clob": CLOB_HOST,
            "data_api": DATA_API_HOST,
        },
        "started_at_utc": _iso_utc(started_at),
        "finished_at_utc": _iso_utc(finished_at),
        "rows_trades": stats.rows_trades,
        "rows_prices": stats.rows_prices,
        "rows_orderbook": stats.rows_orderbook,
        "errors_count": stats.errors_count,
        "warnings": stats.warnings,
        "orderbook_available": bool(include_orderbook and stats.rows_orderbook > 0 and stats.orderbook_available),
        "notes": notes,
        "coverage_start_utc": _iso_utc(day_start),
        "coverage_end_utc": _iso_utc(day_end),
        "collection_status": "ok" if stats.errors_count == 0 else "partial_with_errors",
        "gaps": [w for w in stats.warnings if "market_not_found" in w],
    }
    metadata_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=True), encoding="utf-8")

    return {
        "market": spec.market,
        "trades_path": str(trades_path),
        "prices_path": str(prices_path),
        "orderbook_path": str(orderbook_path) if orderbook_path.exists() else "",
        "metadata_path": str(metadata_path),
        "rows_trades": stats.rows_trades,
        "rows_prices": stats.rows_prices,
        "rows_orderbook": stats.rows_orderbook,
        "errors_count": stats.errors_count,
        "warnings_count": len(stats.warnings),
        "orderbook_available": metadata["orderbook_available"],
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch SOL5M/SOL15M history for a UTC date into data/raw contract.")
    parser.add_argument("--market", choices=["SOL5M", "SOL15M", "both"], default="both")
    parser.add_argument("--date", default=None, help="UTC date YYYY-MM-DD (default: today UTC)")
    parser.add_argument("--out-dir", default="data/raw")
    parser.add_argument("--include-orderbook", default="true", choices=["true", "false"])
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--max-pages", type=int, default=3)
    parser.add_argument("--sleep-ms", type=int, default=120)
    parser.add_argument("--timeout-sec", type=float, default=8.0)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    include_orderbook = _parse_bool(args.include_orderbook)
    collect_date = _parse_date(args.date)
    out_dir = Path(args.out_dir)
    specs = _market_specs(args.market)

    session = requests.Session()
    session.headers.update({"User-Agent": "sol-history-fetcher/1.0"})

    print(f"collect_date_utc={collect_date.isoformat()}")
    print(f"markets={','.join(s.market for s in specs)}")
    print(f"out_dir={out_dir.resolve()}")
    print(f"include_orderbook={include_orderbook}")
    print(
        f"page_size={int(args.page_size)} max_pages={int(args.max_pages)} "
        f"sleep_ms={int(args.sleep_ms)} timeout_sec={float(args.timeout_sec)}"
    )

    summaries: list[dict[str, Any]] = []
    for spec in specs:
        try:
            summary = _collect_market(
                session,
                spec=spec,
                collect_date=collect_date,
                out_dir=out_dir,
                include_orderbook=include_orderbook,
                page_size=int(args.page_size),
                max_pages=int(args.max_pages),
                sleep_ms=int(args.sleep_ms),
                timeout_sec=float(args.timeout_sec),
            )
            summaries.append(summary)
        except Exception as exc:
            print(f"[{spec.market}] fatal_error={exc}")
            summaries.append(
                {
                    "market": spec.market,
                    "rows_trades": 0,
                    "rows_prices": 0,
                    "rows_orderbook": 0,
                    "errors_count": 1,
                    "warnings_count": 1,
                    "orderbook_available": False,
                }
            )

    print("collection_summary:")
    for s in summaries:
        print(
            f"  - {s['market']}: trades={s.get('rows_trades', 0)} prices={s.get('rows_prices', 0)} "
            f"orderbook={s.get('rows_orderbook', 0)} errors={s.get('errors_count', 0)} "
            f"orderbook_available={s.get('orderbook_available', False)}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
