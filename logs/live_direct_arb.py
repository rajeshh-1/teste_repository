import argparse
import asyncio
import csv
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
import websockets
from web3 import Web3

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    # Legacy entrypoint compatibility: allow importing the new package layout.
    sys.path.insert(0, str(_REPO_ROOT))

try:
    from bot.core.config import LIVE_CONFIRM_PHRASE, build_runtime_config, load_env_file, validate_startup
    from bot.core.edge import calculate_edge_from_legs
    from bot.core.pretrade import PreTradeRequest, validate_pretrade
    from bot.core.reason_codes import ACCEPTED, HEDGE_FAILED, KILL_SWITCH_ACTIVE, LEG_TIMEOUT, PARTIAL_FILL
    from bot.core.risk.guards import CircuitBreaker, RiskLimits
    from bot.core.storage.jsonl_logger import JsonlLogger
    from bot.core.storage.sqlite_store import ArbSQLiteStore
    from bot.crypto_updown.runtime.live_runtime import (
        CryptoExecutionRuntime,
        LegExecutionResult,
        LegOrderRequest,
    )
except ModuleNotFoundError:
    from arb_engine.config import LIVE_CONFIRM_PHRASE, build_runtime_config, load_env_file, validate_startup
    from arb_engine.edge import calculate_edge_from_legs
    from arb_engine.pretrade import PreTradeRequest, validate_pretrade
    from arb_engine.jsonl_log import JsonlLogger
    from arb_engine.persistence import ArbSQLiteStore
    ACCEPTED = "accepted"
    PARTIAL_FILL = "partial_fill"
    HEDGE_FAILED = "hedge_failed"
    LEG_TIMEOUT = "leg_timeout"
    KILL_SWITCH_ACTIVE = "kill_switch_active"
    CircuitBreaker = None
    RiskLimits = None
    CryptoExecutionRuntime = None
    LegExecutionResult = None
    LegOrderRequest = None

try:
    from bot.core.execution.kalshi_client import KalshiOrderClient
except Exception:
    try:
        from kalshi_order_client import KalshiOrderClient
    except Exception:
        KalshiOrderClient = None


if LegOrderRequest is None:
    @dataclass(frozen=True)
    class LegOrderRequest:
        leg_name: str
        venue: str
        side: str
        price: float
        quantity: float
        timeout_sec: float


if LegExecutionResult is None:
    @dataclass(frozen=True)
    class LegExecutionResult:
        status: str
        filled_qty: float
        reason_code: str
        detail: str
        elapsed_sec: float = 0.0


if CryptoExecutionRuntime is None:
    class CryptoExecutionRuntime:
        def __init__(self, *, risk_guard: Any, store: Any = None, event_logger: Any = None) -> None:
            self.risk_guard = risk_guard
            self.store = store
            self.event_logger = event_logger

        def execute(self, **kwargs):
            return type(
                "ExecutionDecision",
                (),
                {
                    "accepted": True,
                    "reason_code": ACCEPTED,
                    "detail": "fallback_runtime",
                    "leg_a": None,
                    "leg_b": None,
                    "hedge_attempted": False,
                    "hedge_ok": False,
                },
            )()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_utc_now() -> str:
    return _utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_json_field(value: Any) -> list:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def _normalize_outcome_label(value: Any) -> str:
    txt = str(value or "").strip().lower()
    if txt in {"yes", "up"}:
        return "up"
    if txt in {"no", "down"}:
        return "down"
    return ""


def _edge_from_cost(cost: float) -> float:
    if cost <= 0:
        return -9999.0
    return ((1.0 - cost) / cost) * 100.0


def _ops_expected_add(args: argparse.Namespace) -> float:
    items = [
        (float(args.pess_cancel_timeout_rate), float(args.pess_cancel_timeout_penalty)),
        (float(args.pess_late_fill_rate), float(args.pess_late_fill_penalty)),
        (float(args.pess_reprice_rate), float(args.pess_reprice_penalty)),
        (float(args.pess_tail_rate), float(args.pess_tail_penalty)),
    ]
    out = 0.0
    for rate, pen in items:
        out += max(0.0, min(1.0, rate)) * max(0.0, pen)
    return out


def _ensure_csv(path: Path, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 0:
        return
    with path.open("w", newline="", encoding="utf-8") as fh:
        csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore").writeheader()


def _append_csv(path: Path, columns: list[str], row: dict) -> None:
    with path.open("a", newline="", encoding="utf-8") as fh:
        csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore").writerow(row)


TRADE_COLS = [
    "event_ts_utc",
    "trade_id",
    "status",
    "error_code",
    "opened_at_utc",
    "closed_at_utc",
    "market_key",
    "strategy",
    "edge_conservative_pct",
    "shares",
    "btc_value",
    "poly_entry_side",
    "poly_entry_pct",
    "poly_entry_price",
    "poly_spent_usd",
    "poly_expected_pnl_usd",
    "poly_realized_pnl_usd",
    "kalshi_entry_side",
    "kalshi_entry_pct",
    "kalshi_entry_price",
    "kalshi_spent_usd",
    "kalshi_expected_pnl_usd",
    "kalshi_realized_pnl_usd",
    "total_expected_pnl_usd",
    "total_realized_pnl_usd",
    "outcome_kalshi",
    "outcome_poly",
    "outcome_confirmed",
]

DECISION_COLS = [
    "event_ts_utc",
    "reason_code",
    "detail",
    "market_key_k",
    "market_key_p",
    "strategy",
    "edge_a_pct",
    "edge_b_pct",
    "edge_selected_pct",
]

SECURITY_COLS = [
    "event_ts_utc",
    "severity",
    "code",
    "detail",
    "address",
    "nonce_latest",
    "nonce_pending",
]


@dataclass
class KalshiQuote:
    timestamp_utc: datetime
    market_key: str
    market_close_utc: str
    ticker: str
    floor_strike: Optional[float]
    yes_bid: Optional[float]
    yes_ask: Optional[float]
    no_bid: Optional[float]
    no_ask: Optional[float]
    yes_depth: Optional[float]
    no_depth: Optional[float]


@dataclass
class PolyQuote:
    timestamp_utc: datetime
    market_key: str
    market_close_utc: str
    slug: str
    token_up: str
    token_down: str
    up_best_bid: Optional[float]
    up_best_ask: Optional[float]
    down_best_bid: Optional[float]
    down_best_ask: Optional[float]
    up_ask_liq: Optional[float]
    down_ask_liq: Optional[float]


@dataclass
class Trade:
    trade_id: str
    opened_at_utc: datetime
    market_key: str
    strategy: str
    kalshi_entry_side: str
    poly_entry_side: str
    kalshi_entry_price: float
    poly_entry_price: float
    kalshi_entry_pct: float
    poly_entry_pct: float
    shares: int
    kalshi_spent_usd: float
    poly_spent_usd: float
    kalshi_expected_pnl_usd: float
    poly_expected_pnl_usd: float
    total_expected_pnl_usd: float
    edge_conservative_pct: float
    btc_value: Optional[float]
    ticker: str
    slug: str
    market_close_utc: str
    status: str = "planned"
    error_code: str = ""
    closed_at_utc: Optional[datetime] = None
    kalshi_realized_pnl_usd: float = 0.0
    poly_realized_pnl_usd: float = 0.0
    total_realized_pnl_usd: float = 0.0
    outcome_kalshi: str = ""
    outcome_poly: str = ""
    outcome_confirmed: bool = False

    @property
    def is_open(self) -> bool:
        return self.status not in {"closed", "pending_review"}


@dataclass
class Runtime:
    wallet_k: float
    wallet_p: float
    lock_k: float = 0.0
    lock_p: float = 0.0
    k_quote: Optional[KalshiQuote] = None
    p_quote: Optional[PolyQuote] = None
    open_trade: Optional[Trade] = None
    trade_seq: int = 0
    decisions: dict[str, int] = None
    expected_pnl: float = 0.0
    realized_pnl: float = 0.0
    closed: int = 0
    pending: int = 0
    guard_degraded: bool = False
    last_eval_key: str = ""
    kalshi_client: Optional[Any] = None
    kalshi_live_enabled: bool = False
    store: Optional[ArbSQLiteStore] = None
    event_logger: Optional[JsonlLogger] = None
    risk_guard: Optional[Any] = None
    execution_runtime: Optional[CryptoExecutionRuntime] = None

    def __post_init__(self) -> None:
        if self.decisions is None:
            self.decisions = {}

    @property
    def avail_k(self) -> float:
        return max(0.0, self.wallet_k - self.lock_k)

    @property
    def avail_p(self) -> float:
        return max(0.0, self.wallet_p - self.lock_p)


def _trade_row(t: Trade) -> dict:
    return {
        "event_ts_utc": _iso_utc_now(),
        "trade_id": t.trade_id,
        "status": t.status,
        "error_code": t.error_code,
        "opened_at_utc": t.opened_at_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "closed_at_utc": "" if t.closed_at_utc is None else t.closed_at_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "market_key": t.market_key,
        "strategy": t.strategy,
        "edge_conservative_pct": f"{t.edge_conservative_pct:.6f}",
        "shares": t.shares,
        "btc_value": "" if t.btc_value is None else t.btc_value,
        "poly_entry_side": t.poly_entry_side,
        "poly_entry_pct": f"{t.poly_entry_pct:.6f}",
        "poly_entry_price": f"{t.poly_entry_price:.6f}",
        "poly_spent_usd": f"{t.poly_spent_usd:.6f}",
        "poly_expected_pnl_usd": f"{t.poly_expected_pnl_usd:.6f}",
        "poly_realized_pnl_usd": f"{t.poly_realized_pnl_usd:.6f}",
        "kalshi_entry_side": t.kalshi_entry_side,
        "kalshi_entry_pct": f"{t.kalshi_entry_pct:.6f}",
        "kalshi_entry_price": f"{t.kalshi_entry_price:.6f}",
        "kalshi_spent_usd": f"{t.kalshi_spent_usd:.6f}",
        "kalshi_expected_pnl_usd": f"{t.kalshi_expected_pnl_usd:.6f}",
        "kalshi_realized_pnl_usd": f"{t.kalshi_realized_pnl_usd:.6f}",
        "total_expected_pnl_usd": f"{t.total_expected_pnl_usd:.6f}",
        "total_realized_pnl_usd": f"{t.total_realized_pnl_usd:.6f}",
        "outcome_kalshi": t.outcome_kalshi,
        "outcome_poly": t.outcome_poly,
        "outcome_confirmed": "true" if t.outcome_confirmed else "false",
    }


def _log_decision(
    rt: Runtime,
    decision_csv: Path,
    reason: str,
    detail: str,
    market_key_k: str,
    market_key_p: str,
    strategy: str = "",
    edge_a: Optional[float] = None,
    edge_b: Optional[float] = None,
    edge_sel: Optional[float] = None,
    edge_liquido_pct: Optional[float] = None,
    liq_k: Optional[float] = None,
    liq_p: Optional[float] = None,
) -> None:
    rt.decisions[reason] = int(rt.decisions.get(reason, 0)) + 1
    event_ts = _iso_utc_now()
    _append_csv(
        decision_csv,
        DECISION_COLS,
        {
            "event_ts_utc": event_ts,
            "reason_code": reason,
            "detail": detail,
            "market_key_k": market_key_k,
            "market_key_p": market_key_p,
            "strategy": strategy,
            "edge_a_pct": "" if edge_a is None else f"{edge_a:.6f}",
            "edge_b_pct": "" if edge_b is None else f"{edge_b:.6f}",
            "edge_selected_pct": "" if edge_sel is None else f"{edge_sel:.6f}",
        },
    )
    if rt.store is not None:
        try:
            rt.store.record_skip(
                ts_utc=event_ts,
                reason_code=reason,
                detail=str(detail),
                market_key_k=str(market_key_k),
                market_key_p=str(market_key_p),
                strategy=str(strategy),
                edge_liquido_pct=edge_liquido_pct if edge_liquido_pct is not None else edge_sel,
                liq_k=liq_k,
                liq_p=liq_p,
                metadata={
                    "edge_a_pct": edge_a,
                    "edge_b_pct": edge_b,
                    "edge_selected_pct": edge_sel,
                },
            )
        except Exception:
            pass
    if rt.event_logger is not None:
        try:
            rt.event_logger.log(
                "decision",
                {
                    "ts_utc": event_ts,
                    "reason_code": reason,
                    "detail": detail,
                    "market_key_k": market_key_k,
                    "market_key_p": market_key_p,
                    "strategy": strategy,
                    "edge_a_pct": edge_a,
                    "edge_b_pct": edge_b,
                    "edge_selected_pct": edge_sel,
                    "edge_liquido_pct": edge_liquido_pct,
                    "liq_k": liq_k,
                    "liq_p": liq_p,
                },
            )
        except Exception:
            pass


def _flag_true(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


class _NoopRiskGuard:
    def evaluate_entry(self, *, current_equity: float, open_positions: int):
        return type("GuardDecision", (), {"ok": True, "reason_code": ACCEPTED, "detail": "noop_guard"})()

    def record_trade_result(self, *, realized_pnl: float, current_equity: float) -> None:
        return None

    def snapshot(self) -> dict:
        return {}


def _truncate(text: str, max_len: int = 240) -> str:
    out = str(text or "")
    if len(out) <= max_len:
        return out
    return out[: max(0, max_len - 3)] + "..."


def _persist_trade_open(rt: Runtime, trade: Trade) -> None:
    ts = _iso_utc_now()
    if rt.store is not None:
        try:
            rt.store.record_order(
                ts_utc=ts,
                venue="kalshi",
                trade_id=trade.trade_id,
                market_key=trade.market_key,
                order_id="",
                client_order_id=f"{trade.trade_id}_kalshi",
                side=trade.kalshi_entry_side.lower(),
                action="buy",
                price=trade.kalshi_entry_price,
                quantity=trade.shares,
                status=trade.status,
                metadata={"ticker": trade.ticker},
            )
            rt.store.record_order(
                ts_utc=ts,
                venue="polymarket",
                trade_id=trade.trade_id,
                market_key=trade.market_key,
                order_id="",
                client_order_id=f"{trade.trade_id}_poly",
                side=trade.poly_entry_side.lower(),
                action="buy",
                price=trade.poly_entry_price,
                quantity=trade.shares,
                status=trade.status,
                metadata={"slug": trade.slug},
            )
            rt.store.record_fill(
                ts_utc=ts,
                venue="kalshi",
                trade_id=trade.trade_id,
                market_key=trade.market_key,
                order_id="",
                fill_price=trade.kalshi_entry_price,
                fill_qty=trade.shares,
                fee=0.0,
                metadata={"synthetic": True},
            )
            rt.store.record_fill(
                ts_utc=ts,
                venue="polymarket",
                trade_id=trade.trade_id,
                market_key=trade.market_key,
                order_id="",
                fill_price=trade.poly_entry_price,
                fill_qty=trade.shares,
                fee=0.0,
                metadata={"synthetic": True},
            )
        except Exception:
            pass
    if rt.event_logger is not None:
        try:
            rt.event_logger.log(
                "trade_open",
                {
                    "trade_id": trade.trade_id,
                    "market_key": trade.market_key,
                    "strategy": trade.strategy,
                    "shares": trade.shares,
                    "kalshi_side": trade.kalshi_entry_side,
                    "kalshi_price": trade.kalshi_entry_price,
                    "poly_side": trade.poly_entry_side,
                    "poly_price": trade.poly_entry_price,
                    "edge_conservative_pct": trade.edge_conservative_pct,
                    "expected_pnl": trade.total_expected_pnl_usd,
                },
            )
        except Exception:
            pass


def _persist_trade_close(rt: Runtime, trade: Trade) -> None:
    ts = _iso_utc_now()
    if rt.store is not None:
        try:
            rt.store.record_pnl(
                ts_utc=ts,
                trade_id=trade.trade_id,
                market_key=trade.market_key,
                expected_pnl=trade.total_expected_pnl_usd,
                realized_pnl=trade.total_realized_pnl_usd,
                status=trade.status,
                metadata={
                    "outcome_kalshi": trade.outcome_kalshi,
                    "outcome_poly": trade.outcome_poly,
                    "outcome_confirmed": trade.outcome_confirmed,
                },
            )
        except Exception:
            pass
    if rt.event_logger is not None:
        try:
            rt.event_logger.log(
                "trade_close",
                {
                    "trade_id": trade.trade_id,
                    "market_key": trade.market_key,
                    "status": trade.status,
                    "expected_pnl": trade.total_expected_pnl_usd,
                    "realized_pnl": trade.total_realized_pnl_usd,
                    "outcome_kalshi": trade.outcome_kalshi,
                    "outcome_poly": trade.outcome_poly,
                    "outcome_confirmed": trade.outcome_confirmed,
                    "error_code": trade.error_code,
                },
            )
        except Exception:
            pass


def _extract_kalshi_order_meta(resp: Any) -> tuple[str, str]:
    if not isinstance(resp, dict):
        return "", ""
    order_obj = resp.get("order")
    if isinstance(order_obj, dict):
        oid = str(order_obj.get("order_id") or order_obj.get("id") or "").strip()
        status = str(order_obj.get("status") or "").strip().lower()
        return oid, status
    oid = str(resp.get("order_id") or resp.get("id") or "").strip()
    status = str(resp.get("status") or "").strip().lower()
    return oid, status


def _post_kalshi_order(rt: Runtime, args: argparse.Namespace, trade: Trade) -> tuple[bool, str]:
    client = rt.kalshi_client
    if client is None:
        return False, "kalshi_client_unavailable"
    side = trade.kalshi_entry_side.lower()
    if side not in {"yes", "no"}:
        return False, f"invalid_kalshi_side:{trade.kalshi_entry_side}"
    post_only = _flag_true(getattr(args, "post_only_strict", "true"))
    time_in_force = str(getattr(args, "kalshi_time_in_force", "good_till_canceled")).strip().lower()
    client_order_id = f"{trade.trade_id}_{int(time.time() * 1000)}"
    req: dict[str, Any] = {
        "ticker": trade.ticker,
        "side": side,
        "action": "buy",
        "order_type": "limit",
        "count": int(trade.shares),
        "client_order_id": client_order_id,
        "post_only": post_only,
        "time_in_force": time_in_force,
    }
    px_txt = f"{float(trade.kalshi_entry_price):.4f}"
    if side == "yes":
        req["yes_price_dollars"] = px_txt
    else:
        req["no_price_dollars"] = px_txt
    try:
        resp = client.create_order(**req)
        oid, status = _extract_kalshi_order_meta(resp)
        detail = f"client_order_id={client_order_id}"
        if oid:
            detail += f" order_id={oid}"
        if status:
            detail += f" status={status}"
        if rt.store is not None:
            try:
                rt.store.record_order(
                    ts_utc=_iso_utc_now(),
                    venue="kalshi",
                    trade_id=trade.trade_id,
                    market_key=trade.market_key,
                    order_id=oid,
                    client_order_id=client_order_id,
                    side=trade.kalshi_entry_side.lower(),
                    action="buy",
                    price=trade.kalshi_entry_price,
                    quantity=trade.shares,
                    status=status or "posted",
                    metadata={"live_order": True},
                )
            except Exception:
                pass
        if rt.event_logger is not None:
            try:
                rt.event_logger.log(
                    "kalshi_order_posted",
                    {
                        "trade_id": trade.trade_id,
                        "market_key": trade.market_key,
                        "order_id": oid,
                        "client_order_id": client_order_id,
                        "status": status,
                    },
                )
            except Exception:
                pass
        return True, detail
    except Exception as exc:
        return False, _truncate(str(exc), max_len=240)


class KalshiFeed:
    def __init__(self, args: argparse.Namespace, rt: Runtime):
        self.args = args
        self.rt = rt
        self.session = requests.Session()
        self.mod = None
        self.err = ""
        try:
            import importlib.util

            path = Path(args.watch_kalshi).resolve()
            spec = importlib.util.spec_from_file_location("watch_btc_15m_kalshi", str(path))
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                self.mod = mod
        except Exception as e:
            self.err = str(e)

    async def run(self, stop_evt: asyncio.Event) -> None:
        if self.mod is None:
            raise RuntimeError(f"kalshi_feed_import_failed: {self.err}")
        series = getattr(self.args, "kalshi_series", "KXBTC15M")
        while not stop_evt.is_set():
            try:
                await asyncio.to_thread(self._poll_once, series)
            except Exception:
                pass
            await asyncio.sleep(max(0.05, float(self.args.kalshi_poll_sec)))

    def _poll_once(self, series: str) -> None:
        markets = self.mod.fetch_open_markets(self.session, series)
        if not markets:
            return
        pick = None
        pick_dt = None
        for m in markets:
            close_dt = self.mod.parse_iso_utc(m.get("close_time"))
            if close_dt is None:
                continue
            if pick is None or close_dt < pick_dt:
                pick = m
                pick_dt = close_dt
        if pick is None:
            return
        ticker = str(pick.get("ticker", ""))
        md = self.mod.fetch_market(self.session, ticker)
        yes_depth = None
        no_depth = None
        try:
            ob = self.mod.fetch_orderbook(self.session, ticker, depth=5)
            ob_parsed = self.mod.parse_ob_dollars(ob)
            yes_depth = _safe_float(ob_parsed.get("ob_yes_depth"))
            no_depth = _safe_float(ob_parsed.get("ob_no_depth"))
        except Exception:
            pass
        close_dt = self.mod.parse_iso_utc(md.get("close_time"))
        close_iso = self.mod.to_iso_utc(close_dt)
        key = self.mod.make_market_key(close_iso)
        if not key:
            return
        self.rt.k_quote = KalshiQuote(
            timestamp_utc=_utc_now(),
            market_key=key,
            market_close_utc=close_iso,
            ticker=ticker,
            floor_strike=_safe_float(md.get("floor_strike")),
            yes_bid=_safe_float(md.get("yes_bid_dollars")),
            yes_ask=_safe_float(md.get("yes_ask_dollars")),
            no_bid=_safe_float(md.get("no_bid_dollars")),
            no_ask=_safe_float(md.get("no_ask_dollars")),
            yes_depth=yes_depth,
            no_depth=no_depth,
        )


class PolyFeed:
    def __init__(self, args: argparse.Namespace, rt: Runtime):
        self.args = args
        self.rt = rt
        self.session = requests.Session()
        self.mod = None
        self.err = ""
        self.info: dict[str, str] = {}
        self.books: dict[str, dict[str, dict[float, float]]] = {}
        try:
            import importlib.util

            path = Path(args.watch_poly).resolve()
            spec = importlib.util.spec_from_file_location("watch_btc_15m_poly", str(path))
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                self.mod = mod
        except Exception as e:
            self.err = str(e)

    async def run(self, stop_evt: asyncio.Event) -> None:
        if self.mod is None:
            raise RuntimeError(f"poly_feed_import_failed: {self.err}")
        backoff = 1.0
        while not stop_evt.is_set():
            try:
                await self._refresh_market_info(force=False)
                if not self.info:
                    await asyncio.sleep(1.0)
                    continue
                await self._stream_once(stop_evt)
                backoff = 1.0
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 10.0)

    async def _refresh_market_info(self, force: bool) -> None:
        if (not force) and self.info:
            return
        block_ts = self.mod.current_block_ts()
        slug = self.mod.build_slug(block_ts)
        info = await asyncio.to_thread(self.mod.fetch_market_info, self.session, slug)
        if not info:
            return
        up = str(info.get("token_up", ""))
        down = str(info.get("token_down", ""))
        key = str(info.get("market_key", ""))
        close = str(info.get("market_close_utc", ""))
        if not up or not down or not key:
            return
        self.info = {
            "slug": slug,
            "token_up": up,
            "token_down": down,
            "market_key": key,
            "market_close_utc": close,
        }
        self.books.setdefault(up, {"bids": {}, "asks": {}})
        self.books.setdefault(down, {"bids": {}, "asks": {}})
        await self._bootstrap_book(up)
        await self._bootstrap_book(down)
        self._publish()

    async def _bootstrap_book(self, token_id: str) -> None:
        def _fetch():
            r = self.session.get("https://clob.polymarket.com/book", params={"token_id": token_id}, timeout=8)
            r.raise_for_status()
            return r.json()

        try:
            data = await asyncio.to_thread(_fetch)
        except Exception:
            return
        bids = {}
        asks = {}
        for x in data.get("bids", []):
            px = _safe_float(x.get("price"))
            sz = _safe_float(x.get("size"), 0.0) or 0.0
            if px is not None and sz > 0:
                bids[px] = sz
        for x in data.get("asks", []):
            px = _safe_float(x.get("price"))
            sz = _safe_float(x.get("size"), 0.0) or 0.0
            if px is not None and sz > 0:
                asks[px] = sz
        self.books[token_id] = {"bids": bids, "asks": asks}

    async def _stream_once(self, stop_evt: asyncio.Event) -> None:
        tokens = [self.info["token_up"], self.info["token_down"]]
        async with websockets.connect(
            "wss://ws-subscriptions-clob.polymarket.com/ws/market",
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
            max_size=4_000_000,
        ) as ws:
            await ws.send(json.dumps({"assets_ids": tokens, "type": "Market"}))
            while not stop_evt.is_set():
                raw = await asyncio.wait_for(ws.recv(), timeout=15)
                data = json.loads(raw)
                messages = data if isinstance(data, list) else [data]
                for msg in messages:
                    self._handle(msg)

    def _handle(self, msg: dict) -> None:
        etype = str(msg.get("event_type") or msg.get("type") or "").lower()
        asset = str(msg.get("asset_id") or msg.get("assetId") or msg.get("asset") or "")
        if etype == "book":
            if not asset:
                return
            bids = {}
            asks = {}
            for x in msg.get("bids", []):
                px = _safe_float(x.get("price"))
                sz = _safe_float(x.get("size"), 0.0) or 0.0
                if px is not None and sz > 0:
                    bids[px] = sz
            for x in msg.get("asks", []):
                px = _safe_float(x.get("price"))
                sz = _safe_float(x.get("size"), 0.0) or 0.0
                if px is not None and sz > 0:
                    asks[px] = sz
            self.books[asset] = {"bids": bids, "asks": asks}
            self._publish()
            return
        if etype != "price_change":
            return
        changes = msg.get("changes", [])
        if not isinstance(changes, list):
            return
        if not asset and changes:
            asset = str(changes[0].get("asset_id") or changes[0].get("assetId") or "")
        if not asset:
            return
        book = self.books.setdefault(asset, {"bids": {}, "asks": {}})
        for ch in changes:
            px = _safe_float(ch.get("price"))
            sz = _safe_float(ch.get("size"), 0.0) or 0.0
            if px is None:
                continue
            side = str(ch.get("side") or "").upper()
            if side in {"BUY", "BID"}:
                slot = book["bids"]
            elif side in {"SELL", "ASK"}:
                slot = book["asks"]
            else:
                continue
            if sz <= 0:
                slot.pop(px, None)
            else:
                slot[px] = sz
        self._publish()

    def _publish(self) -> None:
        if not self.info:
            return
        up_id = self.info["token_up"]
        dn_id = self.info["token_down"]
        up = self.books.get(up_id, {"bids": {}, "asks": {}})
        dn = self.books.get(dn_id, {"bids": {}, "asks": {}})
        up_bid = max(up["bids"].keys()) if up["bids"] else None
        up_ask = min(up["asks"].keys()) if up["asks"] else None
        dn_bid = max(dn["bids"].keys()) if dn["bids"] else None
        dn_ask = min(dn["asks"].keys()) if dn["asks"] else None
        up_ask_liq = float(sum(up["asks"].values())) if up["asks"] else 0.0
        dn_ask_liq = float(sum(dn["asks"].values())) if dn["asks"] else 0.0
        self.rt.p_quote = PolyQuote(
            timestamp_utc=_utc_now(),
            market_key=self.info["market_key"],
            market_close_utc=self.info["market_close_utc"],
            slug=self.info["slug"],
            token_up=up_id,
            token_down=dn_id,
            up_best_bid=up_bid,
            up_best_ask=up_ask,
            down_best_bid=dn_bid,
            down_best_ask=dn_ask,
            up_ask_liq=up_ask_liq,
            down_ask_liq=dn_ask_liq,
        )


class NonceGuard:
    def __init__(self, args: argparse.Namespace, security_csv: Path):
        self.args = args
        self.security_csv = security_csv
        self.w3 = None
        self.addresses: list[str] = []
        self.last_nonce: dict[str, tuple[int, int]] = {}
        self.degraded = False
        self._init_guard()

    def _log(self, severity: str, code: str, detail: str, address: str, latest: int, pending: int) -> None:
        _append_csv(
            self.security_csv,
            SECURITY_COLS,
            {
                "event_ts_utc": _iso_utc_now(),
                "severity": severity,
                "code": code,
                "detail": detail,
                "address": address,
                "nonce_latest": latest,
                "nonce_pending": pending,
            },
        )

    def _init_guard(self) -> None:
        if str(self.args.nonce_guard).lower() != "on":
            return
        rpc = os.getenv("POLY_RPC_URL", "https://polygon-rpc.com")
        self.w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 8}))
        if not self.w3.is_connected():
            self.degraded = True
            self.w3 = None
            self._log("warning", "guard_degraded", f"rpc_unreachable={rpc}", "", -1, -1)
            return
        from eth_account import Account

        pk = os.getenv("POLY_PRIVATE_KEY", "").strip()
        funder = os.getenv("POLY_FUNDER", "").strip()
        addrs: list[str] = []
        if pk:
            try:
                addrs.append(Account.from_key(pk).address)
            except Exception:
                pass
        if funder.startswith("0x"):
            addrs.append(funder)
        seen = set()
        out = []
        for x in addrs:
            low = x.lower()
            if low in seen:
                continue
            seen.add(low)
            out.append(x)
        self.addresses = out

    async def run(self, stop_evt: asyncio.Event, rt: Runtime) -> None:
        if str(self.args.nonce_guard).lower() != "on":
            return
        while not stop_evt.is_set():
            try:
                await asyncio.to_thread(self._poll_once, rt)
            except Exception as e:
                if not self.degraded:
                    self.degraded = True
                    rt.guard_degraded = True
                    self._log("warning", "guard_degraded", f"poll_error={e}", "", -1, -1)
            await asyncio.sleep(max(0.5, float(self.args.nonce_poll_sec)))

    def _poll_once(self, rt: Runtime) -> None:
        if self.w3 is None or not self.addresses:
            return
        if self.degraded:
            self.degraded = False
            rt.guard_degraded = False
            self._log("info", "guard_restored", "rpc_ok", "", -1, -1)
        for addr in self.addresses:
            latest = int(self.w3.eth.get_transaction_count(addr, "latest"))
            pending = int(self.w3.eth.get_transaction_count(addr, "pending"))
            prev = self.last_nonce.get(addr.lower())
            if prev is not None:
                if latest > prev[0] or pending > prev[1]:
                    self._log(
                        "warning",
                        "nonce_increase_unexpected",
                        f"prev_latest={prev[0]} prev_pending={prev[1]}",
                        addr,
                        latest,
                        pending,
                    )
            self.last_nonce[addr.lower()] = (latest, pending)


def _fetch_poly_outcome(slug: str) -> tuple[bool, str]:
    try:
        r = requests.get("https://gamma-api.polymarket.com/events", params={"slug": slug}, timeout=8)
        r.raise_for_status()
        events = r.json()
        if not events:
            return False, ""
        event = events[0]
        mkt = None
        for m in event.get("markets", []):
            q = str(m.get("question", "")).lower()
            if "up or down" in q or "up/down" in q or "btc" in q:
                mkt = m
                break
        if mkt is None and event.get("markets"):
            mkt = event["markets"][0]
        if mkt is None or (not bool(mkt.get("closed", False))):
            return False, ""
        outcomes = _load_json_field(mkt.get("outcomes", []))
        prices = _load_json_field(mkt.get("outcomePrices", []))
        if not outcomes or len(outcomes) != len(prices):
            return False, ""
        vals = [_safe_float(p, 0.0) or 0.0 for p in prices]
        idx = max(range(len(vals)), key=lambda i: vals[i])
        if vals[idx] < 0.99:
            return False, ""
        w = _normalize_outcome_label(outcomes[idx])
        return (w in {"up", "down"}), w
    except Exception:
        return False, ""


def _fetch_kalshi_outcome(ticker: str, watch_kalshi_path: str) -> tuple[bool, str]:
    try:
        import importlib.util

        spec = importlib.util.spec_from_file_location("watch_btc_15m_kalshi", str(Path(watch_kalshi_path).resolve()))
        if not spec or not spec.loader:
            return False, ""
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        sess = requests.Session()
        mkt = mod.fetch_market(sess, ticker)
        result = str(mkt.get("result", "")).strip().lower()
        if result in {"yes", "no"}:
            return True, result
        sett = _safe_float(mkt.get("settlement_value"))
        if sett is not None:
            return True, "yes" if sett >= 0.5 else "no"
        return False, ""
    except Exception:
        return False, ""


def _resolve_open_trade(rt: Runtime, args: argparse.Namespace, trades_csv: Path) -> None:
    trade = rt.open_trade
    if trade is None or (not trade.is_open):
        return
    close_ts = pd.to_datetime(trade.market_close_utc, utc=True, errors="coerce")
    if pd.isna(close_ts):
        return
    if _utc_now() < close_ts.to_pydatetime() + pd.Timedelta(seconds=float(args.outcome_poll_grace_sec)):
        return
    ok_k, out_k = _fetch_kalshi_outcome(trade.ticker, args.watch_kalshi)
    ok_p, out_p = _fetch_poly_outcome(trade.slug)
    if not ok_k or not ok_p:
        return

    trade.outcome_kalshi = out_k
    trade.outcome_poly = out_p
    trade.outcome_confirmed = True

    if trade.strategy.startswith("A_"):
        k_wins = out_k == "yes"
        p_wins = out_p == "down"
    else:
        k_wins = out_k == "no"
        p_wins = out_p == "up"

    if k_wins == p_wins:
        trade.status = "pending_review"
        trade.error_code = "official_outcome_mismatch"
        trade.closed_at_utc = _utc_now()
        rt.lock_k = 0.0
        rt.lock_p = 0.0
        rt.pending += 1
        _append_csv(trades_csv, TRADE_COLS, _trade_row(trade))
        _persist_trade_close(rt, trade)
        return

    if k_wins:
        trade.kalshi_realized_pnl_usd = float(trade.shares) - trade.kalshi_spent_usd
        trade.poly_realized_pnl_usd = -trade.poly_spent_usd
    else:
        trade.kalshi_realized_pnl_usd = -trade.kalshi_spent_usd
        trade.poly_realized_pnl_usd = float(trade.shares) - trade.poly_spent_usd
    trade.total_realized_pnl_usd = trade.kalshi_realized_pnl_usd + trade.poly_realized_pnl_usd
    trade.status = "closed"
    trade.closed_at_utc = _utc_now()
    rt.wallet_k += trade.kalshi_realized_pnl_usd
    rt.wallet_p += trade.poly_realized_pnl_usd
    rt.realized_pnl += trade.total_realized_pnl_usd
    rt.lock_k = 0.0
    rt.lock_p = 0.0
    rt.closed += 1
    _append_csv(trades_csv, TRADE_COLS, _trade_row(trade))
    _persist_trade_close(rt, trade)
    if rt.risk_guard is not None:
        try:
            rt.risk_guard.record_trade_result(
                realized_pnl=trade.total_realized_pnl_usd,
                current_equity=rt.wallet_k + rt.wallet_p,
            )
        except Exception:
            pass


def _pretrade_revalidate_now(
    rt: Runtime,
    args: argparse.Namespace,
    *,
    strategy: str,
    market_key: str,
) -> tuple[bool, str, str]:
    kq = rt.k_quote
    pq = rt.p_quote
    if kq is None or pq is None:
        return False, "stale_quotes", "revalidate_missing_feed"
    if kq.market_key != market_key or pq.market_key != market_key:
        return False, "invalid_market_mismatch", "market_key_changed_before_send"

    diff = abs((kq.timestamp_utc - pq.timestamp_utc).total_seconds())
    if diff > float(args.tolerance_sec):
        return False, "stale_quotes", f"revalidate_time_diff_sec={diff:.3f}"

    post_only = str(args.post_only_strict).lower() == "true"
    if strategy.startswith("A_"):
        k_px = kq.yes_bid if post_only else kq.yes_ask
        p_px = pq.down_best_bid if post_only else pq.down_best_ask
        liq_k = _safe_float(kq.yes_depth, 0.0) or 0.0
        liq_p = _safe_float(pq.down_ask_liq, 0.0) or 0.0
    else:
        k_px = kq.no_bid if post_only else kq.no_ask
        p_px = pq.up_best_bid if post_only else pq.up_best_ask
        liq_k = _safe_float(kq.no_depth, 0.0) or 0.0
        liq_p = _safe_float(pq.up_ask_liq, 0.0) or 0.0
    if k_px is None or p_px is None:
        return False, "missing_book_side", "revalidate_required_prices_missing"

    edge = calculate_edge_from_legs(
        kalshi_leg_price=float(k_px),
        poly_leg_price=float(p_px),
        fee_kalshi_bps=float(args.fee_kalshi_bps),
        fee_poly_bps=float(args.fee_poly_bps),
        slippage_expected_bps=float(getattr(args, "slippage_expected_bps", 0.0)),
        custo_leg_risk=float(getattr(args, "leg_risk_cost", 0.0)) + _ops_expected_add(args),
        payout_esperado=float(getattr(args, "payout_esperado", 1.0)),
    )
    pretrade = validate_pretrade(
        PreTradeRequest(
            strategy=strategy,
            market_key_k=kq.market_key,
            market_key_p=pq.market_key,
            semantic_equivalent=(kq.market_key == pq.market_key and kq.market_close_utc == pq.market_close_utc),
            resolution_compatible=True,
            edge=edge,
            min_edge_pct=float(args.min_edge_pct),
            liquidity_k=liq_k,
            liquidity_p=liq_p,
            min_liquidity=float(getattr(args, "min_liquidity", 1.0)),
        )
    )
    return pretrade.ok, pretrade.reason_code, pretrade.detail


def _execute_leg_for_trade(
    rt: Runtime,
    args: argparse.Namespace,
    trade: Trade,
    leg: LegOrderRequest,
) -> LegExecutionResult:
    sim_latency = max(0.0, float(getattr(args, "exec_sim_leg_latency_sec", 0.0)))
    sim_fill_ratio = _clamp01(float(getattr(args, "exec_sim_partial_fill_ratio", 1.0)))

    if leg.venue == "kalshi" and rt.kalshi_live_enabled:
        started = time.monotonic()
        ok, detail = _post_kalshi_order(rt, args, trade)
        elapsed = time.monotonic() - started
        if elapsed > float(leg.timeout_sec):
            return LegExecutionResult(
                status=LEG_TIMEOUT,
                filled_qty=0.0,
                reason_code=LEG_TIMEOUT,
                detail=f"elapsed_sec={elapsed:.4f} > timeout_sec={float(leg.timeout_sec):.4f}",
                elapsed_sec=elapsed,
            )
        if not ok:
            return LegExecutionResult(
                status="rejected",
                filled_qty=0.0,
                reason_code="kalshi_post_failed",
                detail=detail,
                elapsed_sec=elapsed,
            )
        filled_qty = float(leg.quantity) * sim_fill_ratio
        if filled_qty + 1e-9 < float(leg.quantity):
            return LegExecutionResult(
                status=PARTIAL_FILL,
                filled_qty=filled_qty,
                reason_code=PARTIAL_FILL,
                detail=f"simulated_partial_fill ratio={sim_fill_ratio:.4f}",
                elapsed_sec=elapsed,
            )
        return LegExecutionResult(
            status="filled",
            filled_qty=float(leg.quantity),
            reason_code=ACCEPTED,
            detail="kalshi_order_filled",
            elapsed_sec=elapsed,
        )

    elapsed = sim_latency
    if elapsed > float(leg.timeout_sec):
        return LegExecutionResult(
            status=LEG_TIMEOUT,
            filled_qty=0.0,
            reason_code=LEG_TIMEOUT,
            detail=f"elapsed_sec={elapsed:.4f} > timeout_sec={float(leg.timeout_sec):.4f}",
            elapsed_sec=elapsed,
        )
    filled_qty = float(leg.quantity) * sim_fill_ratio
    if filled_qty <= 0:
        return LegExecutionResult(
            status="rejected",
            filled_qty=0.0,
            reason_code="leg_rejected",
            detail="simulated_zero_fill",
            elapsed_sec=elapsed,
        )
    if filled_qty + 1e-9 < float(leg.quantity):
        return LegExecutionResult(
            status=PARTIAL_FILL,
            filled_qty=filled_qty,
            reason_code=PARTIAL_FILL,
            detail=f"simulated_partial_fill ratio={sim_fill_ratio:.4f}",
            elapsed_sec=elapsed,
        )
    return LegExecutionResult(
        status="filled",
        filled_qty=float(leg.quantity),
        reason_code=ACCEPTED,
        detail="filled",
        elapsed_sec=elapsed,
    )


def _hedge_flatten_emergency(rt: Runtime, args: argparse.Namespace, trade: Trade, leg_a: LegExecutionResult, leg_b: LegExecutionResult) -> bool:
    fail = _flag_true(getattr(args, "exec_force_hedge_fail", "false"))
    if rt.event_logger is not None:
        try:
            rt.event_logger.log(
                "hedge_attempt",
                {
                    "trade_id": trade.trade_id,
                    "market_key": trade.market_key,
                    "force_fail": fail,
                    "leg_a_status": leg_a.status,
                    "leg_b_status": leg_b.status,
                    "leg_a_filled_qty": leg_a.filled_qty,
                    "leg_b_filled_qty": leg_b.filled_qty,
                },
            )
        except Exception:
            pass
    return not fail


def _maybe_open_trade(rt: Runtime, args: argparse.Namespace, trades_csv: Path, decisions_csv: Path) -> None:
    kq = rt.k_quote
    pq = rt.p_quote
    if kq is None or pq is None:
        _log_decision(rt, decisions_csv, "missing_feed", "kalshi_or_poly_missing", kq.market_key if kq else "", pq.market_key if pq else "")
        return
    if kq.market_key != pq.market_key:
        _log_decision(rt, decisions_csv, "invalid_market_mismatch", "market_key_diff", kq.market_key, pq.market_key)
        return
    if rt.open_trade is not None and rt.open_trade.is_open:
        _log_decision(rt, decisions_csv, "max_open_trades", "existing_open_trade", kq.market_key, pq.market_key)
        return
    if rt.risk_guard is not None:
        guard_decision = rt.risk_guard.evaluate_entry(
            current_equity=rt.wallet_k + rt.wallet_p,
            open_positions=1 if (rt.open_trade is not None and rt.open_trade.is_open) else 0,
        )
        if not guard_decision.ok:
            reason = KILL_SWITCH_ACTIVE if guard_decision.reason_code == KILL_SWITCH_ACTIVE else "circuit_breaker_triggered"
            _log_decision(
                rt,
                decisions_csv,
                reason,
                guard_decision.detail,
                kq.market_key,
                pq.market_key,
            )
            return

    diff = abs((kq.timestamp_utc - pq.timestamp_utc).total_seconds())
    if diff > float(args.tolerance_sec):
        _log_decision(rt, decisions_csv, "stale_quotes", f"time_diff_sec={diff:.3f}", kq.market_key, pq.market_key)
        return

    sec_key = f"{kq.market_key}:{int(_utc_now().timestamp())}"
    if sec_key == rt.last_eval_key:
        return
    rt.last_eval_key = sec_key

    fee_k = float(args.fee_kalshi_bps) / 10000.0
    fee_p = float(args.fee_poly_bps) / 10000.0
    ops_add = _ops_expected_add(args)
    leg_risk_cost = float(getattr(args, "leg_risk_cost", 0.0)) + ops_add
    slippage_expected_bps = float(getattr(args, "slippage_expected_bps", 0.0))
    payout_expected = float(getattr(args, "payout_esperado", 1.0))
    post_only = str(args.post_only_strict).lower() == "true"

    if post_only:
        k_yes = kq.yes_bid
        k_no = kq.no_bid
        p_up = pq.up_best_bid
        p_down = pq.down_best_bid
    else:
        k_yes = kq.yes_ask
        k_no = kq.no_ask
        p_up = pq.up_best_ask
        p_down = pq.down_best_ask

    if None in {k_yes, k_no, p_up, p_down}:
        _log_decision(rt, decisions_csv, "missing_book_side", "required_prices_missing", kq.market_key, pq.market_key)
        return

    edge_a_res = calculate_edge_from_legs(
        kalshi_leg_price=float(k_yes),
        poly_leg_price=float(p_down),
        fee_kalshi_bps=float(args.fee_kalshi_bps),
        fee_poly_bps=float(args.fee_poly_bps),
        slippage_expected_bps=slippage_expected_bps,
        custo_leg_risk=leg_risk_cost,
        payout_esperado=payout_expected,
    )
    edge_b_res = calculate_edge_from_legs(
        kalshi_leg_price=float(k_no),
        poly_leg_price=float(p_up),
        fee_kalshi_bps=float(args.fee_kalshi_bps),
        fee_poly_bps=float(args.fee_poly_bps),
        slippage_expected_bps=slippage_expected_bps,
        custo_leg_risk=leg_risk_cost,
        payout_esperado=payout_expected,
    )
    edge_a = edge_a_res.edge_liquido_pct
    edge_b = edge_b_res.edge_liquido_pct

    if edge_a >= edge_b:
        strat = "A_KALSHI_YES_PLUS_POLY_DOWN"
        edge = edge_a
        k_side = "YES"
        p_side = "DOWN"
        k_px = float(k_yes)
        p_px = float(p_down)
        edge_res = edge_a_res
        liq_k = _safe_float(kq.yes_depth, 0.0) or 0.0
        liq_p = _safe_float(pq.down_ask_liq, 0.0) or 0.0
    else:
        strat = "B_KALSHI_NO_PLUS_POLY_UP"
        edge = edge_b
        k_side = "NO"
        p_side = "UP"
        k_px = float(k_no)
        p_px = float(p_up)
        edge_res = edge_b_res
        liq_k = _safe_float(kq.no_depth, 0.0) or 0.0
        liq_p = _safe_float(pq.up_ask_liq, 0.0) or 0.0

    semantic_equivalent = (kq.market_key == pq.market_key) and (kq.market_close_utc == pq.market_close_utc)
    resolution_compatible = True
    pretrade = validate_pretrade(
        PreTradeRequest(
            strategy=strat,
            market_key_k=kq.market_key,
            market_key_p=pq.market_key,
            semantic_equivalent=semantic_equivalent,
            resolution_compatible=resolution_compatible,
            edge=edge_res,
            min_edge_pct=float(args.min_edge_pct),
            liquidity_k=liq_k,
            liquidity_p=liq_p,
            min_liquidity=float(getattr(args, "min_liquidity", 1.0)),
        )
    )
    if not pretrade.ok:
        _log_decision(
            rt,
            decisions_csv,
            pretrade.reason_code,
            pretrade.detail,
            kq.market_key,
            pq.market_key,
            strategy=strat,
            edge_a=edge_a,
            edge_b=edge_b,
            edge_sel=edge,
            edge_liquido_pct=edge_res.edge_liquido_pct,
            liq_k=liq_k,
            liq_p=liq_p,
        )
        return

    k_unit = k_px * (1.0 + fee_k)
    p_unit = p_px * (1.0 + fee_p)
    liq_cap = min(liq_k, liq_p)
    shares = int(
        min(
            rt.avail_k / max(1e-12, k_unit),
            rt.avail_p / max(1e-12, p_unit),
            float(args.max_shares_per_trade),
            max(0.0, liq_cap),
        )
    )
    if shares < 1:
        _log_decision(
            rt,
            decisions_csv,
            "budget_blocked",
            "shares_lt_1",
            kq.market_key,
            pq.market_key,
            strategy=strat,
            edge_a=edge_a,
            edge_b=edge_b,
            edge_sel=edge,
            edge_liquido_pct=edge_res.edge_liquido_pct,
            liq_k=liq_k,
            liq_p=liq_p,
        )
        return

    rt.trade_seq += 1
    trade = Trade(
        trade_id=f"T{rt.trade_seq:06d}",
        opened_at_utc=_utc_now(),
        market_key=kq.market_key,
        strategy=strat,
        kalshi_entry_side=k_side,
        poly_entry_side=p_side,
        kalshi_entry_price=k_px,
        poly_entry_price=p_px,
        kalshi_entry_pct=k_px * 100.0,
        poly_entry_pct=p_px * 100.0,
        shares=shares,
        kalshi_spent_usd=shares * k_unit,
        poly_spent_usd=shares * p_unit,
        kalshi_expected_pnl_usd=(0.5 * shares) - (shares * k_unit),
        poly_expected_pnl_usd=(0.5 * shares) - (shares * p_unit),
        total_expected_pnl_usd=shares * edge_res.edge_liquido,
        edge_conservative_pct=edge_res.edge_liquido_pct,
        btc_value=kq.floor_strike,
        ticker=kq.ticker,
        slug=pq.slug,
        market_close_utc=kq.market_close_utc,
    )

    trade.status = "planned"
    _append_csv(trades_csv, TRADE_COLS, _trade_row(trade))

    runtime = rt.execution_runtime or CryptoExecutionRuntime(risk_guard=rt.risk_guard, store=rt.store, event_logger=rt.event_logger)
    rt.execution_runtime = runtime

    leg_a = LegOrderRequest(
        leg_name="leg_a",
        venue="kalshi",
        side=trade.kalshi_entry_side.lower(),
        price=trade.kalshi_entry_price,
        quantity=float(trade.shares),
        timeout_sec=max(0.05, float(getattr(args, "leg_timeout_sec", 2.0))),
    )
    leg_b = LegOrderRequest(
        leg_name="leg_b",
        venue="polymarket",
        side=trade.poly_entry_side.lower(),
        price=trade.poly_entry_price,
        quantity=float(trade.shares),
        timeout_sec=max(0.05, float(getattr(args, "leg_timeout_sec", 2.0))),
    )

    decision = runtime.execute(
        trade_id=trade.trade_id,
        market_key=trade.market_key,
        strategy=trade.strategy,
        current_equity=rt.wallet_k + rt.wallet_p,
        open_positions=0,
        edge_liquido_pct=edge_res.edge_liquido_pct,
        liq_k=liq_k,
        liq_p=liq_p,
        pretrade_revalidate=lambda: _pretrade_revalidate_now(rt, args, strategy=strat, market_key=trade.market_key),
        leg_a=leg_a,
        leg_b=leg_b,
        execute_leg=lambda leg_req: _execute_leg_for_trade(rt, args, trade, leg_req),
        hedge_flatten=lambda leg_a_res, leg_b_res: _hedge_flatten_emergency(rt, args, trade, leg_a_res, leg_b_res),
    )

    if not decision.accepted:
        trade.status = "pending_review"
        trade.error_code = decision.reason_code
        trade.closed_at_utc = _utc_now()
        rt.pending += 1
        _append_csv(trades_csv, TRADE_COLS, _trade_row(trade))
        _log_decision(
            rt,
            decisions_csv,
            decision.reason_code,
            decision.detail,
            kq.market_key,
            pq.market_key,
            strategy=strat,
            edge_a=edge_a,
            edge_b=edge_b,
            edge_sel=edge,
            edge_liquido_pct=edge_res.edge_liquido_pct,
            liq_k=liq_k,
            liq_p=liq_p,
        )
        return

    for st in ("posted", "filled", "open"):
        trade.status = st
        _append_csv(trades_csv, TRADE_COLS, _trade_row(trade))

    _persist_trade_open(rt, trade)
    rt.open_trade = trade
    rt.lock_k = trade.kalshi_spent_usd
    rt.lock_p = trade.poly_spent_usd
    rt.expected_pnl += trade.total_expected_pnl_usd


def _write_summary(path: Path, mode: str, rt: Runtime, started: float) -> None:
    lines = [
        f"mode={mode}",
        f"finished_at_utc={_iso_utc_now()}",
        f"runtime_sec={time.time() - started:.2f}",
        f"wallet_kalshi_usd={rt.wallet_k:.6f}",
        f"wallet_poly_usd={rt.wallet_p:.6f}",
        f"expected_pnl_total_usd={rt.expected_pnl:.6f}",
        f"realized_pnl_total_usd={rt.realized_pnl:.6f}",
        f"trades_closed={rt.closed}",
        f"trades_pending_review={rt.pending}",
        f"guard_degraded={'true' if rt.guard_degraded else 'false'}",
        "decisions_by_reason:",
    ]
    if rt.decisions:
        for k, v in sorted(rt.decisions.items(), key=lambda kv: kv[0]):
            lines.append(f"  - {k}: {v}")
    else:
        lines.append("  - none: 0")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


async def _engine_loop(
    args: argparse.Namespace,
    rt: Runtime,
    trades_csv: Path,
    decisions_csv: Path,
    stop_evt: asyncio.Event,
) -> None:
    started = time.time()
    max_runtime = float(args.runtime_sec)
    while not stop_evt.is_set():
        _maybe_open_trade(rt, args, trades_csv, decisions_csv)
        _resolve_open_trade(rt, args, trades_csv)
        if max_runtime > 0 and (time.time() - started) >= max_runtime:
            stop_evt.set()
            break
        await asyncio.sleep(max(0.05, float(args.eval_interval_sec)))


def _resolve_live_paths(args: argparse.Namespace, output_dir: Path) -> tuple[Path, Path, Path, Path]:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    trades = Path(args.live_trades_csv) if getattr(args, "live_trades_csv", "") else output_dir / f"arb_live_trades_{ts}.csv"
    decs = Path(args.live_decisions_csv) if getattr(args, "live_decisions_csv", "") else output_dir / f"arb_live_decisions_{ts}.csv"
    sec = Path(args.live_security_csv) if getattr(args, "live_security_csv", "") else output_dir / f"arb_live_security_{ts}.csv"
    smy = Path(args.live_summary_file) if getattr(args, "live_summary_file", "") else output_dir / f"arb_live_summary_{ts}.txt"
    return trades.resolve(), decs.resolve(), sec.resolve(), smy.resolve()


def _init_kalshi_live_client(args: argparse.Namespace, mode: str, summary_lines: list[str]) -> tuple[Optional[Any], bool]:
    execution_mode = str(getattr(args, "execution_mode", "paper")).strip().lower()
    requested = _flag_true(getattr(args, "kalshi_order_live", "false"))
    allow_single_leg = _flag_true(getattr(args, "allow_single_leg_risk", "false"))
    summary_lines.append(f"execution_mode={execution_mode}")
    summary_lines.append(f"kalshi_order_live_requested={'true' if requested else 'false'}")
    summary_lines.append(f"allow_single_leg_risk={'true' if allow_single_leg else 'false'}")
    if not requested:
        return None, False
    if execution_mode != "live":
        summary_lines.append("kalshi_order_live_enabled=false")
        summary_lines.append("kalshi_order_live_block_reason=execution_mode_not_live")
        return None, False
    if not allow_single_leg:
        summary_lines.append("kalshi_order_live_enabled=false")
        summary_lines.append("kalshi_order_live_block_reason=single_leg_risk_not_allowed")
        return None, False
    if mode != "live-prod":
        summary_lines.append("kalshi_order_live_enabled=false")
        summary_lines.append("kalshi_order_live_block_reason=only_allowed_in_live_prod")
        return None, False
    if not bool(args.enable_live_prod):
        summary_lines.append("kalshi_order_live_enabled=false")
        summary_lines.append("kalshi_order_live_block_reason=enable_live_prod_false")
        return None, False
    if KalshiOrderClient is None:
        summary_lines.append("kalshi_order_live_enabled=false")
        summary_lines.append("kalshi_order_live_block_reason=kalshi_order_client_import_failed")
        return None, False

    api_key_id = str(getattr(args, "kalshi_api_key_id", "") or os.getenv("KALSHI_API_KEY_ID", "")).strip()
    key_path = str(getattr(args, "kalshi_private_key_path", "") or os.getenv("KALSHI_PRIVATE_KEY_PATH", "")).strip()
    if not api_key_id or not key_path:
        summary_lines.append("kalshi_order_live_enabled=false")
        summary_lines.append("kalshi_order_live_block_reason=missing_kalshi_credentials")
        return None, False
    try:
        client = KalshiOrderClient(
            api_key_id=api_key_id,
            private_key_path=key_path,
            base_url=str(getattr(args, "kalshi_base_url", "https://api.elections.kalshi.com/trade-api/v2")),
            timeout_sec=float(getattr(args, "kalshi_order_timeout_sec", 10.0)),
            sign_path_mode=str(getattr(args, "kalshi_sign_path_mode", "auto")),
        )
    except Exception as exc:
        summary_lines.append("kalshi_order_live_enabled=false")
        summary_lines.append(f"kalshi_order_live_block_reason=client_init_failed:{_truncate(str(exc), 180)}")
        return None, False

    summary_lines.append("kalshi_order_live_enabled=true")
    summary_lines.append(f"kalshi_time_in_force={getattr(args, 'kalshi_time_in_force', 'good_till_canceled')}")
    summary_lines.append(f"kalshi_sign_path_mode={getattr(args, 'kalshi_sign_path_mode', 'auto')}")
    return client, True


async def _run_async(
    args: argparse.Namespace,
    repo_root: Path,
    output_dir: Path,
    summary_lines: list[str],
) -> int:
    mode = str(args.mode)
    if mode == "live-prod" and not bool(args.enable_live_prod):
        summary_lines.append("[live-prod]")
        summary_lines.append("production_locked=true")
        summary_lines.append("production_lock_reason=policy_paused_enable_live_prod_false")
        return 0

    cfg = build_runtime_config(args)
    startup_errors = validate_startup(cfg)
    if startup_errors:
        summary_lines.append("[startup_validation]")
        summary_lines.append("startup_validation_ok=false")
        for err in startup_errors:
            summary_lines.append(f"startup_error={err}")
        return 1

    trades_csv, decisions_csv, security_csv, live_summary = _resolve_live_paths(args, output_dir)
    _ensure_csv(trades_csv, TRADE_COLS)
    _ensure_csv(decisions_csv, DECISION_COLS)
    _ensure_csv(security_csv, SECURITY_COLS)

    store = ArbSQLiteStore(cfg.sqlite_path)
    event_logger = JsonlLogger(cfg.jsonl_path)
    event_logger.log(
        "runtime_start",
        {
            "mode": mode,
            "execution_mode": cfg.execution_mode,
            "min_edge_pct": cfg.min_edge_pct,
            "min_liquidity": cfg.min_liquidity,
            "slippage_expected_bps": cfg.slippage_expected_bps,
            "leg_risk_cost": cfg.leg_risk_cost,
        },
    )

    summary_lines.append(f"[{mode}]")
    kalshi_client, kalshi_live_enabled = _init_kalshi_live_client(args, mode, summary_lines)
    risk_guard = None
    if CircuitBreaker is not None and RiskLimits is not None:
        risk_guard = CircuitBreaker(
            RiskLimits(
                max_losses_streak=int(cfg.max_losses_streak),
                max_daily_drawdown_pct=float(cfg.max_daily_drawdown_pct),
                max_open_positions=int(cfg.max_open_positions),
                kill_switch_path=str(cfg.kill_switch_path),
            ),
            day_start_equity=float(args.max_usd_kalshi) + float(args.max_usd_poly),
        )
    if risk_guard is None:
        risk_guard = _NoopRiskGuard()
    rt = Runtime(
        wallet_k=float(args.max_usd_kalshi),
        wallet_p=float(args.max_usd_poly),
        kalshi_client=kalshi_client,
        kalshi_live_enabled=kalshi_live_enabled,
        store=store,
        event_logger=event_logger,
        risk_guard=risk_guard,
    )
    rt.execution_runtime = CryptoExecutionRuntime(risk_guard=rt.risk_guard, store=store, event_logger=event_logger)
    kfeed = KalshiFeed(args, rt)
    pfeed = PolyFeed(args, rt)
    guard = NonceGuard(args, security_csv)
    stop_evt = asyncio.Event()
    started = time.time()

    summary_lines.append(f"trades_csv={trades_csv}")
    summary_lines.append(f"decisions_csv={decisions_csv}")
    summary_lines.append(f"security_csv={security_csv}")
    summary_lines.append(f"live_summary_file={live_summary}")
    summary_lines.append(f"min_edge_pct={args.min_edge_pct}")
    summary_lines.append(f"max_usd_kalshi={args.max_usd_kalshi}")
    summary_lines.append(f"max_usd_poly={args.max_usd_poly}")
    summary_lines.append(f"max_open_trades={args.max_open_trades}")
    summary_lines.append(f"post_only_strict={args.post_only_strict}")
    summary_lines.append(f"nonce_guard={args.nonce_guard}")
    summary_lines.append(f"nonce_guard_action={args.nonce_guard_action}")
    summary_lines.append(f"poly_feed={args.poly_feed}")
    summary_lines.append(f"kalshi_feed={args.kalshi_feed}")
    summary_lines.append(f"sqlite_file={cfg.sqlite_path}")
    summary_lines.append(f"jsonl_log_file={cfg.jsonl_path}")
    summary_lines.append(f"min_liquidity={cfg.min_liquidity}")
    summary_lines.append(f"slippage_expected_bps={cfg.slippage_expected_bps}")
    summary_lines.append(f"leg_risk_cost={cfg.leg_risk_cost}")
    summary_lines.append(f"payout_esperado={cfg.payout_esperado}")
    summary_lines.append(f"max_losses_streak={cfg.max_losses_streak}")
    summary_lines.append(f"max_daily_drawdown_pct={cfg.max_daily_drawdown_pct}")
    summary_lines.append(f"max_open_positions={cfg.max_open_positions}")
    summary_lines.append(f"kill_switch_path={cfg.kill_switch_path}")

    tasks = [
        asyncio.create_task(pfeed.run(stop_evt), name="poly_feed"),
        asyncio.create_task(kfeed.run(stop_evt), name="kalshi_feed"),
        asyncio.create_task(guard.run(stop_evt, rt), name="nonce_guard"),
        asyncio.create_task(_engine_loop(args, rt, trades_csv, decisions_csv, stop_evt), name="engine"),
    ]
    try:
        await tasks[-1]
    finally:
        stop_evt.set()
        for t in tasks:
            if t.done():
                continue
            t.cancel()
        for t in tasks:
            if t.cancelled():
                continue
            try:
                await t
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        _write_summary(live_summary, mode, rt, started)
        summary_lines.append(f"trades_closed={rt.closed}")
        summary_lines.append(f"trades_pending_review={rt.pending}")
        summary_lines.append(f"expected_pnl_total_usd={rt.expected_pnl:.6f}")
        summary_lines.append(f"realized_pnl_total_usd={rt.realized_pnl:.6f}")
        summary_lines.append(f"wallet_kalshi_usd={rt.wallet_k:.6f}")
        summary_lines.append(f"wallet_poly_usd={rt.wallet_p:.6f}")
        summary_lines.append(f"guard_degraded={'true' if rt.guard_degraded else 'false'}")
        if rt.decisions:
            summary_lines.append("decisions_by_reason:")
            for k, v in sorted(rt.decisions.items(), key=lambda kv: kv[0]):
                summary_lines.append(f"  - {k}: {v}")
        if rt.event_logger is not None:
            rt.event_logger.log(
                "runtime_stop",
                {
                    "mode": mode,
                    "trades_closed": rt.closed,
                    "trades_pending_review": rt.pending,
                    "expected_pnl_total_usd": rt.expected_pnl,
                    "realized_pnl_total_usd": rt.realized_pnl,
                },
            )
        if rt.store is not None:
            rt.store.close()
    return 0


def run_live_mode(args: argparse.Namespace, repo_root: Path, output_dir: Path, summary_lines: list[str]) -> int:
    # v3 lock-ins.
    load_env_file(str((repo_root / ".env").resolve()))
    args.max_open_trades = 1
    args.poly_feed = "ws"
    args.kalshi_feed = "rest"
    return asyncio.run(_run_async(args, repo_root, output_dir, summary_lines))
