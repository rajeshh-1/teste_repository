from dataclasses import dataclass

from bot.core.config import LIVE_CONFIRM_PHRASE


@dataclass(frozen=True)
class SportsRuntimeConfig:
    execution_mode: str
    enable_live_prod: bool
    live_confirmation: str
    min_edge_pct: float
    min_liquidity: float
    max_open_trades: int
    market_scope: str
    sqlite_path: str
    jsonl_path: str


def build_sports_runtime_config(args) -> SportsRuntimeConfig:
    return SportsRuntimeConfig(
        execution_mode=str(getattr(args, "execution_mode", "paper")).strip().lower(),
        enable_live_prod=bool(getattr(args, "enable_live_prod", False)),
        live_confirmation=str(getattr(args, "live_confirmation", "")).strip(),
        min_edge_pct=float(getattr(args, "min_edge_pct", 2.0)),
        min_liquidity=float(getattr(args, "min_liquidity", 1.0)),
        max_open_trades=int(getattr(args, "max_open_trades", 1)),
        market_scope=str(getattr(args, "market_scope", "moneyline")).strip().lower(),
        sqlite_path=str(getattr(args, "sqlite_file", "logs/sports_runtime.sqlite")),
        jsonl_path=str(getattr(args, "jsonl_log_file", "logs/sports_events.jsonl")),
    )


def validate_sports_startup(cfg: SportsRuntimeConfig) -> list[str]:
    errors: list[str] = []
    if cfg.execution_mode not in {"paper", "live"}:
        errors.append("execution_mode must be 'paper' or 'live'")
    if cfg.min_edge_pct < 0:
        errors.append("min_edge_pct must be >= 0")
    if cfg.min_liquidity < 0:
        errors.append("min_liquidity must be >= 0")
    if cfg.max_open_trades < 1:
        errors.append("max_open_trades must be >= 1")
    if not cfg.market_scope:
        errors.append("market_scope is required")

    if cfg.execution_mode == "live":
        if not cfg.enable_live_prod:
            errors.append("live mode requires --enable-live-prod")
        if cfg.live_confirmation != LIVE_CONFIRM_PHRASE:
            errors.append(f"live mode requires --live-confirmation {LIVE_CONFIRM_PHRASE}")
    return errors
