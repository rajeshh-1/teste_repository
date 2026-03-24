import csv
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "build_sol_replay_csv.py"


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


def test_build_sol_replay_csv_generates_required_columns_and_dedup(tmp_path):
    day = "2026-03-24"
    raw_dir = tmp_path / "raw"
    out_dir = tmp_path / "replay"

    _write_jsonl(
        raw_dir / "sol5m" / f"trades_{day}.jsonl",
        [
            {
                "timestamp_utc": "2026-03-24T00:01:00Z",
                "market_key": "SOL5M_2026-03-24T00:05:00Z",
                "price": 0.50,
                "size": 1.0,
                "side": "buy",
                "trade_id": "t1",
            },
            {
                "timestamp_utc": "2026-03-24T00:01:00Z",
                "market_key": "SOL5M_2026-03-24T00:05:00Z",
                "price": 0.51,
                "size": 2.0,
                "side": "sell",
                "trade_id": "t2",
            },
            {
                "timestamp_utc": "2026-03-24T00:03:00Z",
                "market_key": "SOL5M_2026-03-24T00:05:00Z",
                "price": 0.49,
                "size": 1.5,
                "side": "buy",
                "trade_id": "t3",
            },
        ],
    )

    _write_jsonl(
        raw_dir / "sol5m" / f"prices_{day}.jsonl",
        [
            {
                "timestamp_utc": "2026-03-24T00:01:00Z",
                "market_key": "SOL5M_2026-03-24T00:05:00Z",
                "yes_ask": 0.45,
                "no_ask": 0.45,
            }
        ],
    )

    _write_jsonl(
        raw_dir / "sol15m" / f"trades_{day}.jsonl",
        [
            {
                "timestamp_utc": "2026-03-24T00:10:00Z",
                "market_key": "SOL15M_2026-03-24T00:15:00Z",
                "price": 0.50,
                "size": 1.0,
                "side": "buy",
                "trade_id": "t4",
            }
        ],
    )
    _write_jsonl(raw_dir / "sol15m" / f"prices_{day}.jsonl", [])

    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--market",
            "both",
            "--date",
            day,
            "--raw-dir",
            str(raw_dir),
            "--out-dir",
            str(out_dir),
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )

    assert proc.returncode == 0, proc.stdout + "\n" + proc.stderr

    sol5m_out = out_dir / f"sol5m_{day}_replay.csv"
    sol15m_out = out_dir / f"sol15m_{day}_replay.csv"
    assert sol5m_out.exists()
    assert sol15m_out.exists()

    with sol5m_out.open("r", encoding="utf-8", newline="") as fh:
        rows_5m = list(csv.DictReader(fh))
    assert len(rows_5m) == 2
    assert set(rows_5m[0].keys()) == {
        "market_key",
        "timestamp_utc",
        "edge_liq_pct",
        "seconds_to_close",
        "leg2_latency_ms",
        "partial_fill_prob",
        "timeout_prob",
        "hedge_fail_prob",
        "unwind_loss_bps",
    }

    edge_row = next(r for r in rows_5m if r["timestamp_utc"] == "2026-03-24T00:01:00Z")
    assert float(edge_row["edge_liq_pct"]) > 0.0
    fallback_row = next(r for r in rows_5m if r["timestamp_utc"] == "2026-03-24T00:03:00Z")
    assert float(fallback_row["edge_liq_pct"]) == 0.0
    assert int(fallback_row["seconds_to_close"]) >= 0

    with sol15m_out.open("r", encoding="utf-8", newline="") as fh:
        rows_15m = list(csv.DictReader(fh))
    assert len(rows_15m) == 1
    assert rows_15m[0]["market_key"] == "SOL15M_2026-03-24T00:15:00Z"


def test_build_sol_replay_csv_help():
    proc = subprocess.run(
        [sys.executable, str(SCRIPT), "--help"],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert proc.returncode == 0
    assert "--market" in proc.stdout
    assert "--raw-dir" in proc.stdout
    assert "--out-dir" in proc.stdout
