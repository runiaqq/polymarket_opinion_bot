from __future__ import annotations

import csv
import json
import os
import tempfile
from pathlib import Path
from threading import Lock
from typing import Dict, List, Optional

import yaml


class MarketMapper:
    """Maps Polymarket markets to Opinion markets and vice versa."""

    def __init__(self, storage_path: str | Path = "data/mappings.yaml"):
        self.storage_path = Path(storage_path)
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()
        loaded = self.load_mappings(self.storage_path)
        self._pairs: List[Dict[str, object]] = loaded.get("pairs", [])

    @staticmethod
    def load_mappings(path: str | Path) -> Dict[str, object]:
        file = Path(path)
        result = {"pairs": [], "polymarket_to_opinion": {}, "opinion_to_polymarket": {}}
        if not file.exists():
            return result

        suffix = file.suffix.lower()
        if suffix in {".yaml", ".yml"}:
            raw = yaml.safe_load(file.read_text(encoding="utf-8")) or {}
            pairs = raw["pairs"] if isinstance(raw, dict) and "pairs" in raw else raw
        elif suffix == ".csv":
            pairs = []
            with file.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    metadata = row.get("metadata")
                    if metadata:
                        try:
                            metadata = json.loads(metadata)
                        except json.JSONDecodeError:
                            metadata = {"notes": metadata}
                    else:
                        metadata = {}
                    pairs.append(
                        {
                            "polymarket": row.get("polymarket"),
                            "opinion": row.get("opinion"),
                            "metadata": metadata,
                        }
                    )
        else:
            raise ValueError(f"Unsupported mapping file format: {suffix}")

        result["pairs"] = pairs or []
        result["polymarket_to_opinion"] = {
            entry["polymarket"]: entry for entry in result["pairs"] if entry.get("polymarket")
        }
        result["opinion_to_polymarket"] = {
            entry["opinion"]: entry for entry in result["pairs"] if entry.get("opinion")
        }
        return result

    def save_mapping(
        self,
        poly_market_id: str,
        opinion_market_id: str,
        metadata: Optional[Dict[str, object]] = None,
    ) -> None:
        metadata = metadata or {}
        with self._lock:
            existing = next(
                (
                    entry
                    for entry in self._pairs
                    if entry["polymarket"] == poly_market_id or entry["opinion"] == opinion_market_id
                ),
                None,
            )
            if existing:
                existing["polymarket"] = poly_market_id
                existing["opinion"] = opinion_market_id
                existing["metadata"] = metadata
            else:
                self._pairs.append(
                    {
                        "polymarket": poly_market_id,
                        "opinion": opinion_market_id,
                        "metadata": metadata,
                    }
                )
            self._persist()

    def remove_mapping(self, poly_market_id: str | None = None, opinion_market_id: str | None = None) -> bool:
        with self._lock:
            before = len(self._pairs)
            self._pairs = [
                entry
                for entry in self._pairs
                if not (
                    (poly_market_id and entry["polymarket"] == poly_market_id)
                    or (opinion_market_id and entry["opinion"] == opinion_market_id)
                )
            ]
            removed = before != len(self._pairs)
            if removed:
                self._persist()
            return removed

    def find_opinion_for_polymarket(self, poly_id: str) -> Optional[str]:
        for entry in self._pairs:
            if entry["polymarket"] == poly_id:
                return entry["opinion"]
        return None

    def find_polymarket_for_opinion(self, opinion_id: str) -> Optional[str]:
        for entry in self._pairs:
            if entry["opinion"] == opinion_id:
                return entry["polymarket"]
        return None

    def list_mappings(self) -> List[Dict[str, object]]:
        return list(self._pairs)

    def export(self, destination: str | Path, fmt: str = "yaml") -> Path:
        dest = Path(destination)
        dest.parent.mkdir(parents=True, exist_ok=True)
        fmt = fmt.lower()
        if fmt == "csv":
            with dest.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["polymarket", "opinion", "metadata"])
                writer.writeheader()
                for entry in self._pairs:
                    writer.writerow(
                        {
                            "polymarket": entry["polymarket"],
                            "opinion": entry["opinion"],
                            "metadata": json.dumps(entry.get("metadata", {})),
                        }
                    )
        else:
            with dest.open("w", encoding="utf-8") as handle:
                yaml.safe_dump({"pairs": self._pairs}, handle, sort_keys=False)
        return dest

    def save_mapping_from_csv(self, csv_path: str | Path) -> None:
        loaded = self.load_mappings(csv_path)
        for entry in loaded["pairs"]:
            self.save_mapping(entry["polymarket"], entry["opinion"], entry.get("metadata", {}))

    def _persist(self) -> None:
        tmp_dir = self.storage_path.parent
        tmp_dir.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=tmp_dir, suffix=self.storage_path.suffix)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                yaml.safe_dump({"pairs": self._pairs}, handle, sort_keys=False)
            os.replace(tmp_path, self.storage_path)
        except Exception:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise

