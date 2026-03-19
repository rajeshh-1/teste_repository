# Architecture Refactor Plan (Fase 1)

## Scope
Este documento cobre apenas a Fase 1:
- inventário de entrypoints atuais,
- mapeamento de dependências e duplicidades,
- classificação por arquivo (`manter`, `deprecar`, `mover`, `remover`),
- arquitetura-alvo,
- tabela de migração arquivo -> arquivo.

## 1) Inventário de Entrypoints Atuais

### 1.1 Entrypoints operacionais (crypto up/down)
| Arquivo | Tipo | Papel atual | Dependências principais | Observação |
|---|---|---|---|---|
| `run_arb_bot.bat` | menu launcher | orquestra monitores/análise/dry-run | `watch_btc_15m_*`, `logs/analyze_arb.py`, `logs/run_arb_dry_run.py` | principal ponto de uso manual |
| `start_btc_15m_monitors.bat` | launcher | sobe 2 monitores BTC 15m | `watch_btc_15m_kalshi.py`, `watch_btc_15m_poly.py` | duplicado funcional do menu |
| `watch_btc_15m_kalshi.py` | python cli | coleta Kalshi BTC15m para CSV | requests + assinatura Kalshi | já preparado para `.env` |
| `watch_btc_15m_poly.py` | python cli | coleta Polymarket BTC15m para CSV | requests + CLOB/Gamma | pipeline atual usa este feed |
| `logs/analyze_arb.py` | python cli | análise de oportunidades por CSV | pandas | cálculo de edge líquido (fase 1) |
| `logs/run_arb_dry_run.py` | python cli | runner único (replay/live-observe/live-shadow/live-prod-locked) | `logs/live_direct_arb.py`, `logs/analyze_arb.py` | CLI central do fluxo atual |
| `logs/live_direct_arb.py` | runtime | feed direto + decisão/execução controlada | `logs/arb_engine/*`, `kalshi_order_client.py` | núcleo live-shadow/live-prod |

### 1.2 Entrypoints legados ainda presentes (alta duplicidade)
| Arquivo | Domínio | Papel atual | Estado |
|---|---|---|---|
| `Bot_Principal/arbitrage_scanner.py` | sports/legacy | scanner Kalshi x Poly antigo | ativo no repo, fora do fluxo principal |
| `Bot_Principal/live_monitor.py` | sports/legacy | monitor/execução legado | ativo no repo, fora do fluxo principal |
| `live_executor.py` | crypto/poly | executor Polymarket standalone | parcialmente sobreposto ao runtime novo |
| `market_hunter.py`, `mm_bot.py`, `mm_bot_cursor.py`, `live_multi_test.py` | sports/market making | entrypoints independentes | arquitetura paralela ao pipeline novo |
| `watch_all_updown_prices.py`, `watch_future_updown_markets.py` | sports | feeders/monitoramento | não integrados ao runtime novo |
| `crypto_5m_simulator.py`, `crypto_15m_simulator.py` | crypto | simulação | úteis, mas fora da estrutura de domínio |

### 1.3 Núcleo novo já existente (logs/arb_engine)
| Arquivo | Papel |
|---|---|
| `logs/arb_engine/config.py` | config, startup validation, mode gating |
| `logs/arb_engine/edge.py` | fórmula de edge líquido |
| `logs/arb_engine/pretrade.py` | pre-trade validator + reject reasons |
| `logs/arb_engine/persistence.py` | SQLite (`orders`, `fills`, `pnl`, `skips`) |
| `logs/arb_engine/jsonl_log.py` | JSONL estruturado |

## 2) Dependências e Pontos de Entrada (mapa resumido)

### 2.1 Fluxo principal atual (crypto)
1. `run_arb_bot.bat` -> chama monitores e runners.
2. `watch_btc_15m_kalshi.py` + `watch_btc_15m_poly.py` -> geram CSVs.
3. `logs/analyze_arb.py` -> calcula oportunidades.
4. `logs/run_arb_dry_run.py` -> modo replay/live-observe/live-shadow/live-prod-locked.
5. `logs/live_direct_arb.py` -> usa `logs/arb_engine/*` e `logs/kalshi_order_client.py`.

### 2.2 Duplicidade arquitetural identificada
- Pipeline novo: `logs/run_arb_dry_run.py` + `logs/live_direct_arb.py` + `logs/arb_engine/*`.
- Pipelines paralelos legados: `Bot_Principal/*`, `live_executor.py`, `mm_*`, `market_hunter.py`.
- Resultado: múltiplos caminhos de execução com regras e contratos de dados diferentes.

## 3) Classificação de Arquivos (manter/deprecar/mover/remover)

| Arquivo | Classificação | Justificativa |
|---|---|---|
| `run_arb_bot.bat` | manter -> mover | manter como wrapper, mover para `scripts/` |
| `start_btc_15m_monitors.bat` | manter -> mover | manter como atalho, mover para `scripts/` |
| `watch_btc_15m_kalshi.py` | mover | virar módulo de feed crypto, wrapper legado |
| `watch_btc_15m_poly.py` | mover | virar módulo de feed crypto, wrapper legado |
| `logs/analyze_arb.py` | mover | virar módulo de análise crypto + wrapper |
| `logs/run_arb_dry_run.py` | mover | virar CLI principal em `scripts/` |
| `logs/live_direct_arb.py` | mover | virar runtime crypto (execução) |
| `logs/kalshi_order_client.py` | mover | cliente de execução core |
| `logs/arb_engine/*` | mover | núcleo comum para `bot/core/` |
| `live_executor.py` | deprecar -> manter temporário | evitar quebra de uso existente até completar unificação |
| `crypto_5m_simulator.py`, `crypto_15m_simulator.py` | manter -> mover | simuladores de domínio crypto |
| `watch_all_updown_prices.py`, `watch_future_updown_markets.py` | manter -> mover | domínio sports (feeds) |
| `market_hunter.py`, `mm_bot.py`, `mm_bot_cursor.py`, `live_multi_test.py` | deprecar | legado paralelo não integrado ao core novo |
| `Bot_Principal/arbitrage_scanner.py`, `Bot_Principal/live_monitor.py` | deprecar | duplicidade histórica; manter só por compatibilidade |
| `Testes_e_Logs/*.py` | remover (execução) / manter (arquivo histórico) | scripts de debug com risco de credencial hardcoded |

## 4) Arquitetura-Alvo

```text
bot/
  core/
    config.py
    edge.py
    pretrade.py
    execution/
      kalshi_client.py
      dual_leg_engine.py
    storage/
      sqlite_store.py
      jsonl_logger.py
    risk/
      guards.py
  crypto_updown/
    feeds/
      kalshi_feed.py
      poly_feed.py
    matching/
      market_key_matcher.py
    analysis/
      analyzer.py
    runtime/
      live_runtime.py
    simulators/
      sim_5m.py
      sim_15m.py
  sports/
    feeds/
      sports_kalshi_feed.py
      sports_poly_feed.py
    matching/
      sports_event_matcher.py
    runtime/
      sports_runtime.py
scripts/
  arb_cli.py
  start_crypto_monitors.py
tests/
  unit/
  integration/
```

## 5) Tabela de Migração Arquivo -> Arquivo

| Origem | Destino alvo | Ação | Compatibilidade |
|---|---|---|---|
| `logs/arb_engine/config.py` | `bot/core/config.py` | mover | reexport temporário em `logs/arb_engine` |
| `logs/arb_engine/edge.py` | `bot/core/edge.py` | mover | reexport temporário |
| `logs/arb_engine/pretrade.py` | `bot/core/pretrade.py` | mover | reexport temporário |
| `logs/arb_engine/persistence.py` | `bot/core/storage/sqlite_store.py` | mover/refinar | alias temporário |
| `logs/arb_engine/jsonl_log.py` | `bot/core/storage/jsonl_logger.py` | mover/refinar | alias temporário |
| `logs/kalshi_order_client.py` | `bot/core/execution/kalshi_client.py` | mover | import shim |
| `logs/live_direct_arb.py` | `bot/crypto_updown/runtime/live_runtime.py` | mover/refatorar | wrapper em `logs/live_direct_arb.py` |
| `logs/analyze_arb.py` | `bot/crypto_updown/analysis/analyzer.py` | mover/refatorar | wrapper em `logs/analyze_arb.py` |
| `watch_btc_15m_kalshi.py` | `bot/crypto_updown/feeds/kalshi_feed.py` | mover/refatorar | wrapper com warning deprecado |
| `watch_btc_15m_poly.py` | `bot/crypto_updown/feeds/poly_feed.py` | mover/refatorar | wrapper com warning deprecado |
| `logs/run_arb_dry_run.py` | `scripts/arb_cli.py` | mover/refatorar | wrapper legado mantém flags antigas |
| `run_arb_bot.bat` | `scripts/run_arb_bot.bat` | mover | arquivo atual chama novo |
| `start_btc_15m_monitors.bat` | `scripts/start_crypto_monitors.bat` | mover | wrapper legado |
| `crypto_5m_simulator.py` | `bot/crypto_updown/simulators/sim_5m.py` | mover | wrapper legado |
| `crypto_15m_simulator.py` | `bot/crypto_updown/simulators/sim_15m.py` | mover | wrapper legado |
| `watch_future_updown_markets.py` | `bot/sports/feeds/sports_kalshi_feed.py` | mover | wrapper legado |
| `watch_all_updown_prices.py` | `bot/sports/feeds/sports_poly_feed.py` | mover | wrapper legado |
| `live_executor.py` | `bot/crypto_updown/runtime/poly_legacy_executor.py` | deprecar/mover | manter comando antigo por 1 ciclo |
| `Bot_Principal/arbitrage_scanner.py` | `bot/sports/runtime/legacy_scanner.py` | deprecar/mover | somente leitura até sunset |
| `Bot_Principal/live_monitor.py` | `bot/sports/runtime/legacy_live_monitor.py` | deprecar/mover | somente leitura até sunset |

## 6) Critério de pronto da Fase 1
- Inventário de entrypoints concluído.
- Duplicidades mapeadas.
- Arquitetura-alvo formalizada.
- Tabela de migração definida com estratégia de compatibilidade.

