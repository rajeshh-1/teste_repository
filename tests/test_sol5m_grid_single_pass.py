import csv
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "run_sol5m_grid_single_pass.py"


def _write_csv(path: Path) -> None:
    rows = [
        "market_key,edge_liq_pct,seconds_to_close,leg2_latency_ms,partial_fill_prob,timeout_prob,hedge_fail_prob,unwind_loss_bps",
        "SOL5M_001,6.5,110,280,0.10,0.02,0.03,30",
        "SOL5M_002,4.2,95,420,0.15,0.03,0.05,50",
        "SOL5M_003,8.0,75,900,0.20,0.04,0.08,80",
        "SOL5M_004,2.0,45,600,0.25,0.05,0.10,120",
        "SOL5M_005,10.0,130,300,0.05,0.01,0.02,20",
    ]
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def _write_grid(path: Path) -> None:
    payload = {
        "leg2_timeout_ms": [300, 900],
        "min_edge_liq_pct": [1.0, 5.0],
        "max_unwind_loss_bps": [30, 120],
        "entry_cutoff_sec": [30],
        "max_trades_per_market": 1,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _run(seed: int, input_csv: Path, grid_json: Path, out_dir: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--market",
            "SOL5M",
            "--input",
            str(input_csv),
            "--grid-file",
            str(grid_json),
            "--seed",
            str(seed),
            "--out-dir",
            str(out_dir),
            "--search-mode",
            "full",
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def test_sol5m_grid_outputs_required_metrics(tmp_path):
    input_csv = tmp_path / "input.csv"
    grid_json = tmp_path / "grid.json"
    out_dir = tmp_path / "out42"
    _write_csv(input_csv)
    _write_grid(grid_json)
    proc = _run(42, input_csv, grid_json, out_dir)
    assert proc.returncode == 0, proc.stdout + "\n" + proc.stderr

    csv_path = out_dir / "profile_results.csv"
    json_path = out_dir / "profile_results.json"
    summary_path = out_dir / "summary.md"
    assert csv_path.exists()
    assert json_path.exists()
    assert summary_path.exists()

    with csv_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        first = next(reader)
    required_columns = {
        "trades_attempted",
        "trades_accepted",
        "fill_full_rate",
        "partial_fill_rate",
        "timeout_rate",
        "hedge_failed_rate",
        "unwind_count",
        "avg_edge_predicted_pct",
        "avg_edge_captured_pct",
        "edge_capture_ratio",
        "pnl_total",
        "pnl_per_trade",
        "max_drawdown_pct",
        "p95_loss",
        "p99_loss",
        "skip_rate",
        "robustness_score",
        "go_no_go",
    }
    assert required_columns.issubset(set(first.keys()))

    summary = summary_path.read_text(encoding="utf-8")
    assert "Top 10 policies por robustez" in summary
    assert "Perfis perigosos" in summary
    assert "Zona segura sugerida" in summary
    assert "Recomendacao final" in summary


def test_sol5m_grid_seed_stability_42_vs_99(tmp_path):
    input_csv = tmp_path / "input.csv"
    grid_json = tmp_path / "grid.json"
    out42 = tmp_path / "out42"
    out99 = tmp_path / "out99"
    _write_csv(input_csv)
    _write_grid(grid_json)

    proc42 = _run(42, input_csv, grid_json, out42)
    proc99 = _run(99, input_csv, grid_json, out99)
    assert proc42.returncode == 0, proc42.stdout + "\n" + proc42.stderr
    assert proc99.returncode == 0, proc99.stdout + "\n" + proc99.stderr

    def top_ids(path: Path, n: int = 10) -> list[str]:
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            out: list[str] = []
            for idx, row in enumerate(reader):
                if idx >= n:
                    break
                out.append(row["policy_id"])
            return out

    top42 = top_ids(out42 / "profile_results.csv")
    top99 = top_ids(out99 / "profile_results.csv")
    assert top42
    assert top99
    overlap = len(set(top42).intersection(set(top99))) / float(len(set(top42)))
    assert overlap >= 0.2
