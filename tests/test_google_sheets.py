from utils.google_sheets import parse_sheet_pairs
from core.models import ExchangeName


def test_parse_sheet_pairs_two_column():
    rows = [
        ["Polymarket", "Opinion", "size_limit", "contract_type", "strategy_direction"],
        ["poly-event-1", "op-event-1", "2500", "MULTI", "B_TO_A"],
    ]
    specs = parse_sheet_pairs(rows)
    assert len(specs) == 1
    spec = list(specs.values())[0]
    assert spec.pair_cfg.primary_market_id == "poly-event-1"
    assert spec.pair_cfg.secondary_market_id == "op-event-1"
    assert spec.pair_cfg.primary_exchange == ExchangeName.POLYMARKET
    assert spec.pair_cfg.secondary_exchange == ExchangeName.OPINION
    assert spec.size_limit == 2500.0
    assert spec.pair_cfg.contract_type.value == "MULTI"
    assert spec.pair_cfg.strategy_direction.value == "B_TO_A"
    assert spec.fingerprint


def test_parse_sheet_pairs_skips_incomplete_rows():
    rows = [["Polymarket", "Opinion"], ["", "op-event-2"]]
    specs = parse_sheet_pairs(rows)
    assert specs == {}

