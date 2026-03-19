import argparse
import sys
from pathlib import Path
from typing import Optional

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from bot.core.config import build_runtime_config, load_env_file, validate_startup


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crypto Up/Down arbitrage CLI.")
    parser.add_argument("--execution-mode", default="paper", choices=["paper", "live"])
    parser.add_argument("--min-edge-pct", type=float, default=5.0)
    parser.add_argument("--min-liquidity", type=float, default=1.0)

    # Optional passthroughs for startup validation compatibility.
    parser.add_argument("--enable-live-prod", action="store_true")
    parser.add_argument("--live-confirmation", default="")
    parser.add_argument("--payout-esperado", type=float, default=1.0)
    parser.add_argument("--slippage-expected-bps", type=float, default=0.0)
    parser.add_argument("--leg-risk-cost", type=float, default=0.0)
    parser.add_argument("--sqlite-file", default="logs/arb_runtime.sqlite")
    parser.add_argument("--jsonl-log-file", default="logs/arb_events.jsonl")
    parser.add_argument("--kalshi-api-key-id", default="")
    parser.add_argument("--kalshi-private-key-path", default="")
    return parser.parse_args(argv)


def _safe_cfg_view(cfg) -> dict:
    return {
        "domain": "crypto_updown",
        "execution_mode": cfg.execution_mode,
        "enable_live_prod": cfg.enable_live_prod,
        "min_edge_pct": cfg.min_edge_pct,
        "min_liquidity": cfg.min_liquidity,
        "payout_esperado": cfg.payout_esperado,
        "slippage_expected_bps": cfg.slippage_expected_bps,
        "leg_risk_cost": cfg.leg_risk_cost,
        "sqlite_path": cfg.sqlite_path,
        "jsonl_path": cfg.jsonl_path,
        "has_kalshi_api_key_id": bool(cfg.kalshi_api_key_id),
        "has_kalshi_private_key_path": bool(cfg.kalshi_private_key_path),
        "has_poly_private_key": bool(cfg.poly_private_key),
        "has_poly_api_key": bool(cfg.poly_api_key),
        "has_poly_api_secret": bool(cfg.poly_api_secret),
        "has_poly_api_passphrase": bool(cfg.poly_api_passphrase),
    }


def validate_from_namespace(args: argparse.Namespace):
    cfg = build_runtime_config(args)
    errors = validate_startup(cfg)
    return cfg, errors


def main(argv: Optional[list[str]] = None) -> int:
    project_root = Path(__file__).resolve().parents[1]
    load_env_file(str(project_root / ".env"))
    args = parse_args(argv)
    cfg, errors = validate_from_namespace(args)

    print("crypto_cli_config:")
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
