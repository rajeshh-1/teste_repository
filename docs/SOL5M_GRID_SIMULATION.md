# SOL5M Grid Single-Pass Simulation

## Objetivo
Executar replay dry-run de SOL 5m com motor single-pass multi-policy usando grid de parametros.

## Entrypoint
`scripts/run_sol5m_grid_single_pass.py`

## Argumentos da CLI
- `--market`: label de mercado (exemplo: `SOL5M`).
- `--input`: CSV de replay.
- `--grid-file`: JSON do grid de politicas.
- `--seed`: seed deterministica.
- `--out-dir`: diretorio de saida.
- `--search-mode`: `full` ou `successive-halving`.

## Grid Inicial
Arquivo: `configs/sol5m_policy_grid.json`

- `leg2_timeout_ms`: `300..10000` passo `100`.
- `min_edge_liq_pct`: `[0.5,1,2,3,4,5,6,7,8,9,10]`.
- `max_unwind_loss_bps`: `[20,30,50,80,120,160,200]`.
- `entry_cutoff_sec`: `[15,30,45,60,90,120]`.
- `max_trades_per_market`: `1`.

Total no modo `full`: `98 * 11 * 7 * 6 = 45276` politicas.

## Formato minimo do replay CSV
Campos aceitos (a CLI usa fallback quando possivel):
- `market_key`
- `edge_liq_pct` (ou `edge_pct` / `edge_liquido_pct`; fallback por `yes_ask + no_ask`)
- `seconds_to_close` (ou derivado de `timestamp_utc` + `market_close_utc`)
- `leg2_latency_ms`
- `partial_fill_prob` (opcional, default `0.10`)
- `timeout_prob` (opcional, default `0.03`)
- `hedge_fail_prob` (opcional, default `0.03`)
- `unwind_loss_bps` (opcional, default `50.0`)

## Comandos
Help:

```bash
python scripts/run_sol5m_grid_single_pass.py --help
```

Replay full:

```bash
python scripts/run_sol5m_grid_single_pass.py \
  --market SOL5M \
  --input logs/sol5m_replay.csv \
  --grid-file configs/sol5m_policy_grid.json \
  --seed 42 \
  --out-dir reports/sol5m_grid_single_pass \
  --search-mode full
```

Replay com successive-halving:

```bash
python scripts/run_sol5m_grid_single_pass.py \
  --market SOL5M \
  --input logs/sol5m_replay.csv \
  --grid-file configs/sol5m_policy_grid.json \
  --seed 42 \
  --out-dir reports/sol5m_grid_single_pass \
  --search-mode successive-halving
```

## Saidas
No `--out-dir`:
- `profile_results.csv`
- `profile_results.json`
- `summary.md`

`summary.md` inclui top 5 policies por `robustness_score`.
