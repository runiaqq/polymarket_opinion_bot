from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional


@dataclass(slots=True)
class ApprovalRecord:
    match_id: str
    status: str  # approved | rejected
    opinion_event_id: str
    polymarket_event_id: str
    title: str
    decided_at: datetime

    def to_dict(self) -> Dict[str, str]:
        return {
            "status": self.status,
            "opinion_event_id": self.opinion_event_id,
            "polymarket_event_id": self.polymarket_event_id,
            "title": self.title,
            "decided_at": self.decided_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, match_id: str, payload: Dict[str, str]) -> "ApprovalRecord":
        decided_at = datetime.fromisoformat(payload.get("decided_at", datetime.now(tz=timezone.utc).isoformat()))
        return cls(
            match_id=match_id,
            status=payload.get("status", "pending"),
            opinion_event_id=payload.get("opinion_event_id", ""),
            polymarket_event_id=payload.get("polymarket_event_id", ""),
            title=payload.get("title", ""),
            decided_at=decided_at,
        )


class EventApprovalStore:
    """Persists approvals/rejections for discovered events."""

    def __init__(self, path: Path | str | None = None):
        self.path = Path(path) if path else Path("data") / "event_approvals.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._records: Dict[str, ApprovalRecord] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return
        for match_id, payload in data.items():
            self._records[match_id] = ApprovalRecord.from_dict(match_id, payload)

    def _save(self) -> None:
        serializable = {mid: rec.to_dict() for mid, rec in self._records.items()}
        self.path.write_text(json.dumps(serializable, ensure_ascii=False, indent=2), encoding="utf-8")

    def status(self, match_id: str) -> Optional[str]:
        record = self._records.get(match_id)
        return record.status if record else None

    def mark(self, match_id: str, status: str, *, opinion_event_id: str, polymarket_event_id: str, title: str) -> None:
        record = ApprovalRecord(
            match_id=match_id,
            status=status,
            opinion_event_id=opinion_event_id,
            polymarket_event_id=polymarket_event_id,
            title=title,
            decided_at=datetime.now(tz=timezone.utc),
        )
        self._records[match_id] = record
        self._save()

    def mark_approved(self, match_id: str, *, opinion_event_id: str, polymarket_event_id: str, title: str) -> None:
        self.mark(match_id, "approved", opinion_event_id=opinion_event_id, polymarket_event_id=polymarket_event_id, title=title)

    def mark_rejected(self, match_id: str, *, opinion_event_id: str, polymarket_event_id: str, title: str) -> None:
        self.mark(match_id, "rejected", opinion_event_id=opinion_event_id, polymarket_event_id=polymarket_event_id, title=title)

    def is_approved(self, match_id: str) -> bool:
        return self.status(match_id) == "approved"

    def is_rejected(self, match_id: str) -> bool:
        return self.status(match_id) == "rejected"

    def export(self) -> Dict[str, ApprovalRecord]:
        return dict(self._records)


__all__ = ["EventApprovalStore", "ApprovalRecord"]

