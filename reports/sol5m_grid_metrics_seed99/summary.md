# SOL5M Single-Pass Grid Summary

- market: SOL5M
- search_mode: full
- input_file: C:\Users\Gstangari\Downloads\Arbitrage sports (2)\Arbitrage sports\Arbitrage sports\reports\sol5m_grid_single_pass_smoke\input_short.csv
- policies_evaluated: 45276
- go_profiles: 43810
- go_limits: hedge_failed_rate<=0.0150, pnl_per_trade>0, max_drawdown_pct<=12.0000, p99_loss<=0.4000

## Top 10 policies por robustez
| policy_id | score | pnl_per_trade | hedge_failed_rate | max_drawdown_pct | p99_loss | go_no_go |
|---|---:|---:|---:|---:|---:|---|
| policy_40ea8202c571 | 0.996494 | 0.599650 | 0.000000 | 0.000000 | 0.000000 | go |
| policy_e67ead026da4 | 0.996494 | 0.599650 | 0.000000 | 0.000000 | 0.000000 | go |
| policy_e91ee398b8d8 | 0.996494 | 0.599650 | 0.000000 | 0.000000 | 0.000000 | go |
| policy_6a8d35a381f5 | 0.996494 | 0.599650 | 0.000000 | 0.000000 | 0.000000 | go |
| policy_eb28ed518627 | 0.996494 | 0.599650 | 0.000000 | 0.000000 | 0.000000 | go |
| policy_3b52d68e59e5 | 0.996494 | 0.599650 | 0.000000 | 0.000000 | 0.000000 | go |
| policy_8d522dc0fb1a | 0.996494 | 0.599650 | 0.000000 | 0.000000 | 0.000000 | go |
| policy_c32098b80581 | 0.996494 | 0.599650 | 0.000000 | 0.000000 | 0.000000 | go |
| policy_591b1d1106b4 | 0.996494 | 0.599650 | 0.000000 | 0.000000 | 0.000000 | go |
| policy_eebdf7ae70cc | 0.996494 | 0.599650 | 0.000000 | 0.000000 | 0.000000 | go |

## Top perfis (Top 5)
- policy_40ea8202c571: score=0.996494, pnl_per_trade=0.599650, hedge_failed_rate=0.000000, max_drawdown_pct=0.000000, p99_loss=0.000000, go_no_go=go
- policy_e67ead026da4: score=0.996494, pnl_per_trade=0.599650, hedge_failed_rate=0.000000, max_drawdown_pct=0.000000, p99_loss=0.000000, go_no_go=go
- policy_e91ee398b8d8: score=0.996494, pnl_per_trade=0.599650, hedge_failed_rate=0.000000, max_drawdown_pct=0.000000, p99_loss=0.000000, go_no_go=go
- policy_6a8d35a381f5: score=0.996494, pnl_per_trade=0.599650, hedge_failed_rate=0.000000, max_drawdown_pct=0.000000, p99_loss=0.000000, go_no_go=go
- policy_eb28ed518627: score=0.996494, pnl_per_trade=0.599650, hedge_failed_rate=0.000000, max_drawdown_pct=0.000000, p99_loss=0.000000, go_no_go=go

## Perfis perigosos (Bottom 5)
- policy_0e424453a90d: score=0.320964, pnl_per_trade=0.017787, hedge_failed_rate=0.000000, max_drawdown_pct=0.516853, p99_loss=0.198880, go_no_go=go
- policy_b7afe4597471: score=0.320964, pnl_per_trade=0.017787, hedge_failed_rate=0.000000, max_drawdown_pct=0.516853, p99_loss=0.198880, go_no_go=go
- policy_23474bd9f94a: score=0.356334, pnl_per_trade=-0.074000, hedge_failed_rate=0.000000, max_drawdown_pct=0.370000, p99_loss=0.168840, go_no_go=no_go
- policy_a6186610579c: score=0.364070, pnl_per_trade=-0.080600, hedge_failed_rate=0.000000, max_drawdown_pct=0.403000, p99_loss=0.195720, go_no_go=no_go
- policy_e0b0469cdf5c: score=0.368271, pnl_per_trade=0.028787, hedge_failed_rate=0.000000, max_drawdown_pct=0.462186, p99_loss=0.198880, go_no_go=go

## Zona segura sugerida
- zona_segura: leg2_timeout_ms=7900..10000, min_edge_liq_pct=0.50..2.00, max_unwind_loss_bps=20.00..200.00, entry_cutoff_sec=15..45

## Recomendacao final
- conservador: policy_40ea8202c571
- moderado: policy_40ea8202c571
- agressivo: policy_40ea8202c571
