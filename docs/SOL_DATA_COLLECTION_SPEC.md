# SOL Data Collection Spec

## Scope
- Target markets: `SOL5M` and `SOL15M`.
- Bootstrap phase: directory and artifact contract only (no external API integration).
- Canonical timezone: `UTC`.

## Directory Layout
- `data/raw/sol5m/`
- `data/raw/sol15m/`
- `data/replay/`

## File Naming Pattern
- `trades_YYYY-MM-DD.jsonl`
- `prices_YYYY-MM-DD.jsonl`
- `orderbook_YYYY-MM-DD.jsonl` (if available)
- `metadata_YYYY-MM-DD.json`

## Minimum Required Fields
### trades
- `timestamp_utc`
- `market_key`
- `price`
- `size`
- `side`
- `trade_id`

### prices
- `timestamp_utc`
- `market_key`
- `best_bid`
- `best_ask`
- `mid`

### orderbook
- `timestamp_utc`
- `market_key`
- `bids`
- `asks`
- `snapshot_id`

### metadata
- `collection_status`
- `gaps`
- `errors`
- `source`
- `coverage_start_utc`
- `coverage_end_utc`

## Data Quality Rules
- Prices must be in range `[0, 1]`.
- All timestamps must be valid UTC timestamps.
- Deduplication must use natural keys:
  - trades: `market_key + trade_id`
  - prices: `market_key + timestamp_utc`
  - orderbook: `market_key + snapshot_id`
- Collection gaps must be explicitly recorded in metadata (`gaps`).

## Bootstrap Outputs
- Empty versioned directories with `.gitkeep`.
- Machine-readable schema in `configs/sol_collection_schema.json`.

## Execution (Step 2)
CLI:

```bash
python scripts/fetch_sol_today_history.py --help
```

Example (today UTC, both markets):

```bash
python scripts/fetch_sol_today_history.py \
  --market both \
  --date YYYY-MM-DD \
  --out-dir data/raw \
  --include-orderbook true
```

Main options:
- `--market SOL5M|SOL15M|both` (default `both`)
- `--date YYYY-MM-DD` (default: current UTC date)
- `--out-dir data/raw`
- `--include-orderbook true|false`
- `--page-size`, `--max-pages`, `--sleep-ms`, `--timeout-sec`

Generated files per market:
- `trades_YYYY-MM-DD.jsonl`
- `prices_YYYY-MM-DD.jsonl`
- `orderbook_YYYY-MM-DD.jsonl` (if collected)
- `metadata_YYYY-MM-DD.json`
