import argparse
import sys
from pathlib import Path
from typing import Optional

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from bot.core.config import load_env_file
from bot.sports.config import build_sports_runtime_config, validate_sports_startup


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sports arbitrage CLI.")
    parser.add_argument("--execution-mode", default="paper", choices=["paper", "live"])
    parser.add_argument("--enable-live-prod", action="store_true")
    parser.add_argument("--live-confirmation", default="")
    parser.add_argument("--min-edge-pct", type=float, default=2.0)
    parser.add_argument("--min-liquidity", type=float, default=1.0)
    parser.add_argument("--max-open-trades", type=int, default=1)
    parser.add_argument("--market-scope", default="moneyline")
    parser.add_argument("--sqlite-file", default="logs/sports_runtime.sqlite")
    parser.add_argument("--jsonl-log-file", default="logs/sports_events.jsonl")
    return parser.parse_args(argv)


def validate_from_namespace(args: argparse.Namespace):
    cfg = build_sports_runtime_config(args)
    errors = validate_sports_startup(cfg)
    return cfg, errors


def _safe_cfg_view(cfg) -> dict:
    return {
        "domain": "sports",
        "execution_mode": cfg.execution_mode,
        "enable_live_prod": cfg.enable_live_prod,
        "min_edge_pct": cfg.min_edge_pct,
        "min_liquidity": cfg.min_liquidity,
        "max_open_trades": cfg.max_open_trades,
        "market_scope": cfg.market_scope,
        "sqlite_path": cfg.sqlite_path,
        "jsonl_path": cfg.jsonl_path,
    }


def main(argv: Optional[list[str]] = None) -> int:
    project_root = Path(__file__).resolve().parents[1]
    load_env_file(str(project_root / ".env"))
    args = parse_args(argv)
    cfg, errors = validate_from_namespace(args)

    print("sports_cli_config:")
    for k, v in _safe_cfg_view(cfg).items():
        print(f"  {k}={v}")

    if errors:
        print("config_validated=false")
        for err in errors:
            print(f"config_error={err}")
        return 1

    print("config_validated=true")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
