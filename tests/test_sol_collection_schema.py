import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "configs" / "sol_collection_schema.json"


def _assert_required_fields(schema: dict, dataset_name: str, expected_fields: list[str]) -> None:
    assert dataset_name in schema["datasets"], f"missing dataset '{dataset_name}'"
    dataset = schema["datasets"][dataset_name]
    required = dataset["required_fields"]
    for field in expected_fields:
        assert field in required, f"missing required field '{field}' in {dataset_name}"
        assert bool(required[field].get("required")) is True, f"field '{field}' is not marked required"


def test_schema_contains_minimum_contract():
    assert SCHEMA_PATH.exists(), "schema file not found"
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))

    _assert_required_fields(
        schema,
        "trades",
        ["timestamp_utc", "market_key", "price", "size", "side", "trade_id"],
    )
    _assert_required_fields(
        schema,
        "prices",
        ["timestamp_utc", "market_key", "best_bid", "best_ask", "mid"],
    )
    _assert_required_fields(
        schema,
        "orderbook",
        ["timestamp_utc", "market_key", "bids", "asks", "snapshot_id"],
    )
    _assert_required_fields(
        schema,
        "metadata",
        ["collection_status", "gaps", "errors", "source", "coverage_start_utc", "coverage_end_utc"],
    )
