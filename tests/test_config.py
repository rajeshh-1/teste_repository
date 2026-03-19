import os
from types import SimpleNamespace

from bot.core.config import LIVE_CONFIRM_PHRASE, build_runtime_config, load_env_file, validate_startup


def _args(**overrides):
    base = dict(
        execution_mode="paper",
        enable_live_prod=False,
        live_confirmation="",
        min_edge_pct=5.0,
        min_liquidity=1.0,
        payout_esperado=1.0,
        slippage_expected_bps=0.0,
        leg_risk_cost=0.0,
        sqlite_file="logs/arb_runtime.sqlite",
        jsonl_log_file="logs/arb_events.jsonl",
        kalshi_api_key_id="",
        kalshi_private_key_path="",
        max_losses_streak=3,
        max_daily_drawdown_pct=20.0,
        max_open_positions=1,
        kill_switch_path="logs/kill_switch.flag",
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def test_live_config_fails_without_confirmation(monkeypatch):
    monkeypatch.setenv("POLY_PRIVATE_KEY", "poly-key")
    cfg = build_runtime_config(
        _args(
            execution_mode="live",
            enable_live_prod=True,
            live_confirmation="WRONG",
            kalshi_api_key_id="kalshi-key",
            kalshi_private_key_path="C:/tmp/key.pem",
        )
    )
    errors = validate_startup(cfg)
    assert f"live mode requires --live-confirmation {LIVE_CONFIRM_PHRASE}" in errors


def test_live_config_fails_without_required_credentials(monkeypatch):
    monkeypatch.delenv("KALSHI_API_KEY_ID", raising=False)
    monkeypatch.delenv("KALSHI_PRIVATE_KEY_PATH", raising=False)
    monkeypatch.delenv("POLY_PRIVATE_KEY", raising=False)
    cfg = build_runtime_config(
        _args(
            execution_mode="live",
            enable_live_prod=True,
            live_confirmation=LIVE_CONFIRM_PHRASE,
            kalshi_api_key_id="",
            kalshi_private_key_path="",
        )
    )
    errors = validate_startup(cfg)
    assert "missing KALSHI_API_KEY_ID for live mode" in errors
    assert "missing KALSHI_PRIVATE_KEY_PATH for live mode" in errors
    assert "missing POLY_PRIVATE_KEY for live mode" in errors


def test_live_config_requires_enable_live_prod_flag(monkeypatch):
    monkeypatch.setenv("POLY_PRIVATE_KEY", "poly-key")
    cfg = build_runtime_config(
        _args(
            execution_mode="live",
            enable_live_prod=False,
            live_confirmation=LIVE_CONFIRM_PHRASE,
            kalshi_api_key_id="kalshi-key",
            kalshi_private_key_path="C:/tmp/key.pem",
        )
    )
    errors = validate_startup(cfg)
    assert "live mode requires --enable-live-prod" in errors


def test_validate_startup_rejects_invalid_numeric_values():
    cfg = build_runtime_config(
        _args(
            execution_mode="invalid-mode",
            min_edge_pct=-1.0,
            min_liquidity=-1.0,
            slippage_expected_bps=-1.0,
            leg_risk_cost=-1.0,
            payout_esperado=0.0,
            max_losses_streak=0,
            max_daily_drawdown_pct=0.0,
            max_open_positions=0,
        )
    )
    errors = validate_startup(cfg)
    assert "execution_mode must be 'paper' or 'live'" in errors
    assert "min_edge_pct must be >= 0" in errors
    assert "min_liquidity must be >= 0" in errors
    assert "slippage_expected_bps must be >= 0" in errors
    assert "leg_risk_cost must be >= 0" in errors
    assert "payout_esperado must be > 0" in errors
    assert "max_losses_streak must be >= 1" in errors
    assert "max_daily_drawdown_pct must be > 0" in errors
    assert "max_open_positions must be >= 1" in errors


def test_load_env_file_parses_lines_and_overwrite(tmp_path, monkeypatch):
    env_file = tmp_path / "test.env"
    env_file.write_text(
        "\n".join(
            [
                "# comment",
                "KALSHI_API_KEY_ID=from_file",
                "POLY_API_KEY =  quoted-value  ",
                "INVALID_LINE_WITHOUT_EQUAL",
                "   ",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("KALSHI_API_KEY_ID", "existing")

    load_env_file(str(env_file), overwrite=False)
    assert os.getenv("KALSHI_API_KEY_ID") == "existing"
    assert os.getenv("POLY_API_KEY") == "quoted-value"

    load_env_file(str(env_file), overwrite=True)
    assert os.getenv("KALSHI_API_KEY_ID") == "from_file"
