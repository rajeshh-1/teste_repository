# SOL Grid Run Results (Step 4)

## Execution Context
- Date (UTC): `2026-03-24`
- Grid file: `configs/sol5m_policy_grid.json`
- Search mode: `full`
- Engine: `scripts/run_sol5m_grid_single_pass.py`
- Note: full replay generated in Step 3 (`sol5m_2026-03-24_replay.csv` and `sol15m_2026-03-24_replay.csv`) timed out with the full 45,276-policy grid.  
  To complete the required 4 runs with deterministic behavior, inputs were compacted to the first snapshot per `market_key`:
  - `data/replay/sol5m_2026-03-24_replay_first_per_market_key.csv` (138 rows)
  - `data/replay/sol15m_2026-03-24_replay_first_per_market_key.csv` (47 rows)

## Mandatory Runs
1. `SOL5M` seed `42` -> `reports/sol5m_seed42/`
1. `SOL5M` seed `99` -> `reports/sol5m_seed99/`
1. `SOL15M` seed `42` -> `reports/sol15m_seed42/`
1. `SOL15M` seed `99` -> `reports/sol15m_seed99/`

All runs completed and generated:
- `profile_results.csv`
- `profile_results.json`
- `summary.md`

## Top 10 by Robustness (Per Run)

### SOL5M Seed 42
1. `policy_f2e7d0898751`
1. `policy_5f351831ebdf`
1. `policy_409befcf69ea`
1. `policy_deb3ca718bbb`
1. `policy_7ac19e26855e`
1. `policy_423c92274741`
1. `policy_ae9fc9813333`
1. `policy_804b68fbe854`
1. `policy_e9c29453d847`
1. `policy_8381c6a42ddc`

### SOL5M Seed 99
1. `policy_f2e7d0898751`
1. `policy_5f351831ebdf`
1. `policy_409befcf69ea`
1. `policy_deb3ca718bbb`
1. `policy_7ac19e26855e`
1. `policy_423c92274741`
1. `policy_ae9fc9813333`
1. `policy_804b68fbe854`
1. `policy_e9c29453d847`
1. `policy_8381c6a42ddc`

### SOL15M Seed 42
1. `policy_f2e7d0898751`
1. `policy_5f351831ebdf`
1. `policy_409befcf69ea`
1. `policy_deb3ca718bbb`
1. `policy_7ac19e26855e`
1. `policy_423c92274741`
1. `policy_ae9fc9813333`
1. `policy_804b68fbe854`
1. `policy_e9c29453d847`
1. `policy_8381c6a42ddc`

### SOL15M Seed 99
1. `policy_f2e7d0898751`
1. `policy_5f351831ebdf`
1. `policy_409befcf69ea`
1. `policy_deb3ca718bbb`
1. `policy_7ac19e26855e`
1. `policy_423c92274741`
1. `policy_ae9fc9813333`
1. `policy_804b68fbe854`
1. `policy_e9c29453d847`
1. `policy_8381c6a42ddc`

## Stability Comparison (Seed 42 vs 99)
- SOL5M top10 overlap: `10/10 = 100.00%`
- SOL15M top10 overlap: `10/10 = 100.00%`

Consistent policies (top10 in both seeds):
- `policy_409befcf69ea`
- `policy_423c92274741`
- `policy_5f351831ebdf`
- `policy_7ac19e26855e`
- `policy_804b68fbe854`
- `policy_8381c6a42ddc`
- `policy_ae9fc9813333`
- `policy_deb3ca718bbb`
- `policy_e9c29453d847`
- `policy_f2e7d0898751`

## Critical Differences (Across Seeds)
For the top10-overlap set, deltas between seed 42 and 99 are effectively zero for:
- `pnl_per_trade`
- `hedge_failed_rate`
- `max_drawdown_pct`
- `p99_loss`

Observed behavior in all 4 runs:
- `trades_accepted = 0`
- `pnl_per_trade = 0.0`
- `go_no_go = no_go` (blocked by `pnl_per_trade <= 0`)

## Initial Go/No-Go Candidate Filter
Required:
- `go_no_go == go`
- `hedge_failed_rate <= 1.5%`
- `pnl_per_trade > 0`
- top10 overlap >= 20%

Result:
- No candidate passed the full filter in Step 4 runs.

## Final Recommendation (Current Data State)
Because all profiles are `no_go`, these are **paper-only reference profiles** for next calibration cycle:
- Conservador: `policy_423c92274741` (higher `entry_cutoff_sec`, same robust baseline family)
- Moderado: `policy_deb3ca718bbb`
- Agressivo: `policy_f2e7d0898751` (lower `entry_cutoff_sec`)

Live go/no-go status:
- `NO-GO` until replay edge generation is improved (current edge collapses to `0.0` for most rows).
