import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


LIVE_CONFIRM_PHRASE = "I_UNDERSTAND_LIVE_ARB"


def load_env_file(path: str = ".env", overwrite: bool = False) -> None:
    env_path = Path(path)
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if not key:
            continue
        if overwrite or key not in os.environ:
            os.environ[key] = value


def _env_or(value: Optional[str], env_key: str, default: str = "") -> str:
    if value is not None and str(value).strip():
        return str(value).strip()
    return str(os.getenv(env_key, default)).strip()


@dataclass(frozen=True)
class RuntimeConfig:
    execution_mode: str
    enable_live_prod: bool
    live_confirmation: str
    min_edge_pct: float
    min_liquidity: float
    payout_esperado: float
    slippage_expected_bps: float
    leg_risk_cost: float
    sqlite_path: str
    jsonl_path: str
    kalshi_api_key_id: str
    kalshi_private_key_path: str
    poly_private_key: str
    poly_api_key: str
    poly_api_secret: str
    poly_api_passphrase: str
    max_losses_streak: int
    max_daily_drawdown_pct: float
    max_open_positions: int
    kill_switch_path: str


def build_runtime_config(args) -> RuntimeConfig:
    return RuntimeConfig(
        execution_mode=str(getattr(args, "execution_mode", "paper")).strip().lower(),
        enable_live_prod=bool(getattr(args, "enable_live_prod", False)),
        live_confirmation=str(getattr(args, "live_confirmation", "")).strip(),
        min_edge_pct=float(getattr(args, "min_edge_pct", 5.0)),
        min_liquidity=float(getattr(args, "min_liquidity", 1.0)),
        payout_esperado=float(getattr(args, "payout_esperado", 1.0)),
        slippage_expected_bps=float(getattr(args, "slippage_expected_bps", 0.0)),
        leg_risk_cost=float(getattr(args, "leg_risk_cost", 0.0)),
        sqlite_path=str(getattr(args, "sqlite_file", "logs/arb_runtime.sqlite")),
        jsonl_path=str(getattr(args, "jsonl_log_file", "logs/arb_events.jsonl")),
        kalshi_api_key_id=_env_or(getattr(args, "kalshi_api_key_id", ""), "KALSHI_API_KEY_ID"),
        kalshi_private_key_path=_env_or(getattr(args, "kalshi_private_key_path", ""), "KALSHI_PRIVATE_KEY_PATH"),
        poly_private_key=_env_or(None, "POLY_PRIVATE_KEY"),
        poly_api_key=_env_or(None, "POLY_API_KEY"),
        poly_api_secret=_env_or(None, "POLY_API_SECRET"),
        poly_api_passphrase=_env_or(None, "POLY_API_PASSPHRASE"),
        max_losses_streak=int(getattr(args, "max_losses_streak", 3)),
        max_daily_drawdown_pct=float(getattr(args, "max_daily_drawdown_pct", 20.0)),
        max_open_positions=int(getattr(args, "max_open_positions", 1)),
        kill_switch_path=str(
            getattr(args, "kill_switch_path", _env_or(None, "ARB_KILL_SWITCH_PATH", "logs/kill_switch.flag"))
        ),
    )


def validate_startup(cfg: RuntimeConfig) -> list[str]:
    errors: list[str] = []
    if cfg.execution_mode not in {"paper", "live"}:
        errors.append("execution_mode must be 'paper' or 'live'")
    if cfg.min_edge_pct < 0:
        errors.append("min_edge_pct must be >= 0")
    if cfg.min_liquidity < 0:
        errors.append("min_liquidity must be >= 0")
    if cfg.slippage_expected_bps < 0:
        errors.append("slippage_expected_bps must be >= 0")
    if cfg.leg_risk_cost < 0:
        errors.append("leg_risk_cost must be >= 0")
    if cfg.payout_esperado <= 0:
        errors.append("payout_esperado must be > 0")
    if cfg.max_losses_streak < 1:
        errors.append("max_losses_streak must be >= 1")
    if cfg.max_daily_drawdown_pct <= 0:
        errors.append("max_daily_drawdown_pct must be > 0")
    if cfg.max_open_positions < 1:
        errors.append("max_open_positions must be >= 1")

    if cfg.execution_mode == "live":
        if not cfg.enable_live_prod:
            errors.append("live mode requires --enable-live-prod")
        if cfg.live_confirmation != LIVE_CONFIRM_PHRASE:
            errors.append(f"live mode requires --live-confirmation {LIVE_CONFIRM_PHRASE}")
        if not cfg.kalshi_api_key_id:
            errors.append("missing KALSHI_API_KEY_ID for live mode")
        if not cfg.kalshi_private_key_path:
            errors.append("missing KALSHI_PRIVATE_KEY_PATH for live mode")
        # Dual-venue arb: keep explicit dependency in startup.
        if not cfg.poly_private_key:
            errors.append("missing POLY_PRIVATE_KEY for live mode")
    return errors
