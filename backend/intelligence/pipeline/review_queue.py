"""Append-only manual review queue (JSON lines)."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_QUEUE_PATH = Path(__file__).resolve().parent.parent / "data" / "review_queue.jsonl"


@dataclass
class ReviewQueueEntry:
    dealer_id: str = ""
    root_url: str = ""
    final_url: str = ""
    reason: str = ""
    flags: list[str] = field(default_factory=list)
    rule_confidence: float = 0.0
    ai_confidence: float | None = None
    best_candidate: str | None = None
    evidence_digest: dict[str, Any] = field(default_factory=dict)
    created_at: str = ""

    def __post_init__(self) -> None:
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()


class ReviewQueue:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or DEFAULT_QUEUE_PATH

    def enqueue(self, entry: ReviewQueueEntry) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(asdict(entry), ensure_ascii=False) + "\n"
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(line)

    def load_recent(self, max_lines: int = 500) -> list[dict[str, Any]]:
        if not self.path.is_file():
            return []
        lines = self.path.read_text(encoding="utf-8").strip().splitlines()
        out: list[dict[str, Any]] = []
        for line in lines[-max_lines:]:
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return out
