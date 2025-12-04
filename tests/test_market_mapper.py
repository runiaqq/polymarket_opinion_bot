from __future__ import annotations

import json
from pathlib import Path

from core.market_mapper import MarketMapper


def test_market_mapper_add_find_export(tmp_path):
    storage = tmp_path / "mappings.yaml"
    mapper = MarketMapper(storage)

    mapper.save_mapping("POLY-1", "OP-1", {"note": "first"})
    mapper.save_mapping("POLY-2", "OP-2", {"note": "second"})

    assert mapper.find_opinion_for_polymarket("POLY-1") == "OP-1"
    assert mapper.find_polymarket_for_opinion("OP-2") == "POLY-2"

    mappings = mapper.list_mappings()
    assert len(mappings) == 2

    export_csv = tmp_path / "export.csv"
    mapper.export(export_csv, fmt="csv")
    loaded = MarketMapper.load_mappings(export_csv)
    assert loaded["polymarket_to_opinion"]["POLY-1"]["opinion"] == "OP-1"

    export_yaml = tmp_path / "export.yaml"
    mapper.export(export_yaml, fmt="yaml")
    assert export_yaml.exists()

    mapper.remove_mapping(poly_market_id="POLY-2")
    assert mapper.find_opinion_for_polymarket("POLY-2") is None


def test_market_mapper_load_from_csv(tmp_path):
    csv_path = tmp_path / "seed.csv"
    csv_path.write_text(
        "polymarket,opinion,metadata\nPOLY-X,OP-X,\"" + json.dumps({"tag": "x"}) + "\"\n",
        encoding="utf-8",
    )
    mapper = MarketMapper(tmp_path / "mappings.yaml")
    mapper.save_mapping_from_csv(csv_path)
    assert mapper.find_opinion_for_polymarket("POLY-X") == "OP-X"

