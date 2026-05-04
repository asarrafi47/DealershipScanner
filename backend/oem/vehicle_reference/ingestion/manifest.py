"""Required provenance for every bulk ingest (CSV, JSON, or API-derived batches)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class ManifestError(ValueError):
    pass


@dataclass(frozen=True)
class SourceManifest:
    source_label: str
    source_url_or_path: str
    market: str
    model_year_min: int
    model_year_max: int
    notes: str = ""

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> SourceManifest:
        if not isinstance(raw, dict):
            raise ManifestError("source_manifest must be an object")
        label = (raw.get("source_label") or "").strip()
        ref = (raw.get("source_url_or_path") or raw.get("source_url") or "").strip()
        market = (raw.get("market") or "").strip() or "US"
        notes = (raw.get("notes") or "").strip()
        try:
            y0 = int(raw["model_year_min"])
            y1 = int(raw["model_year_max"])
        except (KeyError, TypeError, ValueError) as e:
            raise ManifestError("model_year_min and model_year_max must be integers") from e
        if not label:
            raise ManifestError("source_label is required")
        if not ref:
            raise ManifestError("source_url_or_path (or source_url) is required")
        if y0 > y1:
            raise ManifestError("model_year_min must be <= model_year_max")
        return cls(
            source_label=label,
            source_url_or_path=ref,
            market=market,
            model_year_min=y0,
            model_year_max=y1,
            notes=notes,
        )

    def assert_row_year_in_range(self, model_year: int) -> None:
        if not (self.model_year_min <= model_year <= self.model_year_max):
            raise ManifestError(
                f"Row model_year {model_year} outside manifest range "
                f"{self.model_year_min}-{self.model_year_max}"
            )


def require_vehicle_source(obj: dict[str, Any]) -> dict[str, Any]:
    """Each vehicle row must carry its own source block (label + url or local path)."""
    src = obj.get("source")
    if not isinstance(src, dict):
        raise ManifestError("Each vehicle requires a 'source' object with label and url (or path)")
    label = (src.get("label") or "").strip()
    url = (src.get("url") or src.get("local_document_path") or "").strip()
    if not label or not url:
        raise ManifestError("Vehicle source requires non-empty label and url (or local_document_path)")
    return src
