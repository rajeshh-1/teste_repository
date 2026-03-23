# SOL5M Single-Pass Grid Summary

- market: SOL5M
- search_mode: full
- input_file: C:\Users\Gstangari\Downloads\Arbitrage sports (2)\Arbitrage sports\Arbitrage sports\reports\sol5m_grid_single_pass_smoke\input_short.csv
- policies_evaluated: 45276
- go_profiles: 43838
- go_limits: hedge_failed_rate<=0.0150, pnl_per_trade>0, max_drawdown_pct<=12.0000, p99_loss<=0.4000

## Top 10 policies por robustez
| policy_id | score | pnl_per_trade | hedge_failed_rate | max_drawdown_pct | p99_loss | go_no_go |
|---|---:|---:|---:|---:|---:|---|
| policy_bd178137eec5 | 0.996494 | 0.599650 | 0.000000 | 0.000000 | 0.000000 | go |
| policy_401361409823 | 0.996494 | 0.599650 | 0.000000 | 0.000000 | 0.000000 | go |
| policy_0741d7eec033 | 0.996494 | 0.599650 | 0.000000 | 0.000000 | 0.000000 | go |
| policy_14059f439d05 | 0.996494 | 0.599650 | 0.000000 | 0.000000 | 0.000000 | go |
| policy_9da8685f9ed3 | 0.996494 | 0.599650 | 0.000000 | 0.000000 | 0.000000 | go |
| policy_f97c0ab04406 | 0.996494 | 0.599650 | 0.000000 | 0.000000 | 0.000000 | go |
| policy_6a8d35a381f5 | 0.996494 | 0.599650 | 0.000000 | 0.000000 | 0.000000 | go |
| policy_9b8b8ffecda1 | 0.996494 | 0.599650 | 0.000000 | 0.000000 | 0.000000 | go |
| policy_1ff4dc3ace3a | 0.996494 | 0.599650 | 0.000000 | 0.000000 | 0.000000 | go |
| policy_eb28ed518627 | 0.996494 | 0.599650 | 0.000000 | 0.000000 | 0.000000 | go |

## Top perfis (Top 5)
- policy_bd178137eec5: score=0.996494, pnl_per_trade=0.599650, hedge_failed_rate=0.000000, max_drawdown_pct=0.000000, p99_loss=0.000000, go_no_go=go
- policy_401361409823: score=0.996494, pnl_per_trade=0.599650, hedge_failed_rate=0.000000, max_drawdown_pct=0.000000, p99_loss=0.000000, go_no_go=go
- policy_0741d7eec033: score=0.996494, pnl_per_trade=0.599650, hedge_failed_rate=0.000000, max_drawdown_pct=0.000000, p99_loss=0.000000, go_no_go=go
- policy_14059f439d05: score=0.996494, pnl_per_trade=0.599650, hedge_failed_rate=0.000000, max_drawdown_pct=0.000000, p99_loss=0.000000, go_no_go=go
- policy_9da8685f9ed3: score=0.996494, pnl_per_trade=0.599650, hedge_failed_rate=0.000000, max_drawdown_pct=0.000000, p99_loss=0.000000, go_no_go=go

## Perfis perigosos (Bottom 5)
- policy_49a5914d4ea7: score=0.280460, pnl_per_trade=-0.110000, hedge_failed_rate=0.000000, max_drawdown_pct=0.550000, p99_loss=0.198880, go_no_go=no_go
- policy_69e3d589d1b4: score=0.320243, pnl_per_trade=-0.048345, hedge_failed_rate=0.000000, max_drawdown_pct=0.505659, p99_loss=0.198880, go_no_go=no_go
- policy_6fbe01478b73: score=0.359485, pnl_per_trade=0.023497, hedge_failed_rate=0.000000, max_drawdown_pct=0.519672, p99_loss=0.197400, go_no_go=go
- policy_c5df79d26ad7: score=0.382352, pnl_per_trade=0.028787, hedge_failed_rate=0.000000, max_drawdown_pct=0.462186, p99_loss=0.198880, go_no_go=go
- policy_e02b3f0da4ca: score=0.382352, pnl_per_trade=0.028787, hedge_failed_rate=0.000000, max_drawdown_pct=0.462186, p99_loss=0.198880, go_no_go=go

## Zona segura sugerida
- zona_segura: leg2_timeout_ms=8000..10000, min_edge_liq_pct=0.50..2.00, max_unwind_loss_bps=20.00..200.00, entry_cutoff_sec=15..45

## Recomendacao final
- conservador: policy_bd178137eec5
- moderado: policy_bd178137eec5
- agressivo: policy_bd178137eec5
